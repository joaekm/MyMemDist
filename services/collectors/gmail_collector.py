"""
Gmail Collector - Google Gmail Integration

Hämtar e-post med specifik label och sparar som textfiler.

Output: Assets/Mail/Mail_YYYY-MM-DD_Subject_[UUID].txt
Format: Rich header + body text

Princip: HARDFAIL > Silent Fallback
"""

import os
import re
import time
import yaml
import logging
import datetime
import uuid
import zoneinfo
import base64
from email.utils import parsedate_to_datetime

# --- CONFIG ---
def load_yaml(filnamn, strict=True):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    paths = [
        os.path.join(script_dir, '..', '..', 'config', filnamn),
        os.path.join(script_dir, '..', 'config', filnamn),
        os.path.join(script_dir, 'config', filnamn),
    ]
    for p in paths:
        if os.path.exists(p):
            with open(p, 'r') as f:
                return yaml.safe_load(f)
    if strict:
        print(f"[CRITICAL] Kunde inte hitta: {filnamn}")
        exit(1)
    return {}


CONFIG = load_yaml('my_mem_config.yaml', strict=True)

TZ_NAME = CONFIG.get('system', {}).get('timezone', 'UTC')
try:
    SYSTEM_TZ = zoneinfo.ZoneInfo(TZ_NAME)
except Exception as e:
    print(f"[CRITICAL] HARDFAIL: Ogiltig timezone '{TZ_NAME}': {e}")
    exit(1)

MAIL_FOLDER = os.path.expanduser(CONFIG['paths'].get('asset_mail', '~/MyMemory/Assets/Mail'))
LOG_FILE = os.path.expanduser(CONFIG['logging'].get('system_log', '~/MyMemory/Logs/system.log'))
GOOGLE_CONF = CONFIG.get('google', {})
GMAIL_CONF = GOOGLE_CONF.get('gmail', {})

log_dir = os.path.dirname(LOG_FILE)
os.makedirs(log_dir, exist_ok=True)

# Configure root logger: file only, no console
_root = logging.getLogger()
_root.setLevel(logging.INFO)
for _h in _root.handlers[:]:
    _root.removeHandler(_h)
_fh = logging.FileHandler(LOG_FILE)
_fh.setFormatter(logging.Formatter('%(asctime)s - GMAIL - %(levelname)s - %(message)s'))
_root.addHandler(_fh)

LOGGER = logging.getLogger('MyMem_GmailCollector')

# Guard: avsluta rent om Google inte är konfigurerat
if not GOOGLE_CONF.get('credentials_path'):
    _LOGGER.info("google.credentials_path saknas i config — Gmail Collector inaktiv")
    exit(0)
if not GOOGLE_CONF.get('token_path'):
    _LOGGER.info("google.token_path saknas i config — Gmail Collector inaktiv")
    exit(0)

CREDENTIALS_PATH = os.path.expanduser(GOOGLE_CONF['credentials_path'])
TOKEN_PATH = os.path.expanduser(GOOGLE_CONF['token_path'])

if not os.path.exists(CREDENTIALS_PATH):
    _LOGGER.info(f"Google credentials-fil saknas ({CREDENTIALS_PATH}) — Gmail Collector inaktiv")
    exit(0)

SCOPES = GOOGLE_CONF.get('scopes', ['https://www.googleapis.com/auth/gmail.readonly'])
TARGET_LABEL = GMAIL_CONF.get('target_label', 'MyMem')
# HISTORY_DAYS borttagen - hämtar ALLA mail med rätt label, deduplicering hanterar resten

# Tysta tredjepartsloggers
for _name in ['httpx', 'httpcore', 'google', 'googleapiclient', 'oauth2client', 'urllib3']:
    logging.getLogger(_name).setLevel(logging.WARNING)

# Terminal status
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from services.utils.terminal_status import service_status, status as terminal_status


# --- GOOGLE AUTH ---

def get_gmail_service():
    """
    Autentisera mot Gmail API.
    Returnerar en service-instans eller None vid fel.
    """
    try:
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build
    except ImportError as e:
        LOGGER.error(f"HARDFAIL: Google API bibliotek saknas: {e}")
        return None
    
    creds = None
    
    # Ladda befintlig token
    if os.path.exists(TOKEN_PATH):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
        except Exception as e:
            LOGGER.warning(f"Kunde inte ladda token: {e}")
    
    # Refresh eller ny auth
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                LOGGER.warning(f"Kunde inte refresha token: {e}")
                creds = None
        
        if not creds:
            if not os.path.exists(CREDENTIALS_PATH):
                LOGGER.error(f"HARDFAIL: Credentials saknas: {CREDENTIALS_PATH}")
                return None
            
            try:
                flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
                creds = flow.run_local_server(port=0)
            except Exception as e:
                LOGGER.error(f"HARDFAIL: OAuth-flöde misslyckades: {e}")
                return None
        
        # Spara token
        try:
            os.makedirs(os.path.dirname(TOKEN_PATH), exist_ok=True)
            with open(TOKEN_PATH, 'w') as token:
                token.write(creds.to_json())
        except Exception as e:
            LOGGER.warning(f"Kunde inte spara token: {e}")
    
    try:
        service = build('gmail', 'v1', credentials=creds)
        return service
    except Exception as e:
        LOGGER.error(f"HARDFAIL: Kunde inte skapa Gmail service: {e}")
        return None


# --- LABEL HANDLING ---

def get_label_id(service, label_name: str) -> str | None:
    """
    Hämta label ID från label-namn.
    Hanterar nested labels (t.ex. "Digitalist/MyMem").
    """
    try:
        results = service.users().labels().list(userId='me').execute()
        labels = results.get('labels', [])
        
        for label in labels:
            if label['name'] == label_name:
                return label['id']
        
        LOGGER.warning(f"Label '{label_name}' hittades inte")
        return None
    except Exception as e:
        LOGGER.error(f"HARDFAIL: Kunde inte lista labels: {e}")
        return None


# --- MESSAGE EXTRACTION ---

def sanitize_filename(text: str, max_length: int = 50) -> str:
    """
    Rensa text för användning i filnamn.
    """
    # Ta bort ogiltiga tecken
    clean = re.sub(r'[<>:"/\\|?*\n\r]', '', text)
    # Ersätt mellanslag med understreck
    clean = clean.replace(' ', '_')
    # Begränsa längd
    if len(clean) > max_length:
        clean = clean[:max_length]
    return clean or "Utan_amne"


def extract_body_text(payload: dict) -> str:
    """
    Extrahera textinnehåll från Gmail message payload.
    Hanterar multipart-meddelanden.
    """
    body_text = ""
    
    # Enkel text
    if payload.get('mimeType') == 'text/plain':
        data = payload.get('body', {}).get('data', '')
        if data:
            try:
                body_text = base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
            except Exception as e:
                LOGGER.debug(f"Kunde inte avkoda body: {e}")
    
    # Multipart
    elif 'parts' in payload:
        for part in payload['parts']:
            if part.get('mimeType') == 'text/plain':
                data = part.get('body', {}).get('data', '')
                if data:
                    try:
                        body_text = base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
                        break  # Ta första text/plain-delen
                    except Exception as e:
                        LOGGER.debug(f"Kunde inte avkoda part: {e}")
            # Rekursivt för nested multipart
            elif 'parts' in part:
                nested_text = extract_body_text(part)
                if nested_text:
                    body_text = nested_text
                    break
    
    return body_text.strip()


def get_header_value(headers: list, name: str) -> str:
    """Hämta värde för en specifik header."""
    for header in headers:
        if header.get('name', '').lower() == name.lower():
            return header.get('value', '')
    return ''


def fetch_message_details(service, message_id: str) -> dict | None:
    """
    Hämta fullständiga detaljer för ett meddelande.
    """
    try:
        message = service.users().messages().get(
            userId='me',
            id=message_id,
            format='full'
        ).execute()
        
        payload = message.get('payload', {})
        headers = payload.get('headers', [])
        
        # Extrahera headers
        subject = get_header_value(headers, 'Subject') or '(Inget ämne)'
        from_addr = get_header_value(headers, 'From')
        to_addr = get_header_value(headers, 'To')
        date_str = get_header_value(headers, 'Date')
        
        # Parsa datum
        try:
            date_dt = parsedate_to_datetime(date_str)
            if date_dt.tzinfo is None:
                date_dt = date_dt.replace(tzinfo=SYSTEM_TZ)
        except Exception as e:
            LOGGER.debug(f"Kunde inte parsa datum '{date_str}': {e}")
            date_dt = datetime.datetime.now(SYSTEM_TZ)
        
        # Extrahera body
        body = extract_body_text(payload)
        
        return {
            'id': message_id,
            'subject': subject,
            'from': from_addr,
            'to': to_addr,
            'date': date_dt,
            'body': body,
            'thread_id': message.get('threadId', ''),
            'snippet': message.get('snippet', '')
        }
        
    except Exception as e:
        LOGGER.error(f"HARDFAIL: Kunde inte hämta meddelande {message_id}: {e}")
        raise RuntimeError(f"HARDFAIL: Gmail API fel") from e


# --- DEDUPLICATION ---

def get_existing_message_ids() -> set:
    """
    Hämta MESSAGE_IDs för redan sparade e-post.
    Läser från filnamn (format: Mail_YYYY-MM-DD_Subject_UUID.txt).
    """
    existing_ids = set()
    
    try:
        # Vi sparar message_id i filens metadata-header
        # Läs befintliga filer och extrahera MESSAGE_ID
        for f in os.listdir(MAIL_FOLDER):
            if f.endswith('.txt'):
                filepath = os.path.join(MAIL_FOLDER, f)
                try:
                    with open(filepath, 'r', encoding='utf-8') as file:
                        # Läs första 20 raderna för att hitta MESSAGE_ID
                        for _ in range(20):
                            line = file.readline()
                            if line.startswith('MESSAGE_ID:'):
                                msg_id = line.split(':', 1)[1].strip()
                                existing_ids.add(msg_id)
                                break
                except Exception as e:
                    LOGGER.debug(f"Kunde inte läsa {f}: {e}")
    except FileNotFoundError:
        LOGGER.debug(f"Mail folder finns inte: {MAIL_FOLDER}")
    
    return existing_ids


# --- FILE CREATION ---

def create_email_file(message: dict) -> bool:
    """
    Skapa en textfil för ett e-postmeddelande.
    
    Returns:
        True om fil skapades, False om den redan fanns
    """
    date_str = message['date'].strftime('%Y-%m-%d')
    subject_clean = sanitize_filename(message['subject'])
    unit_id = str(uuid.uuid4())
    
    filename = f"Mail_{date_str}_{subject_clean}_{unit_id}.txt"
    filepath = os.path.join(MAIL_FOLDER, filename)
    
    archived_at = datetime.datetime.now(SYSTEM_TZ).isoformat()
    date_iso = message['date'].isoformat()
    
    content = f"""================================================================================
METADATA FRÅN E-POST
================================================================================
MESSAGE_ID:    {message['id']}
ÄMNE:          {message['subject']}
FRÅN:          {message['from']}
TILL:          {message['to']}
DATUM_TID:     {date_iso}
ARKIVERAD:     {archived_at}
KÄLLA:         Gmail API (label: {TARGET_LABEL})
UNIT_ID:       {unit_id}
--------------------------------------------------------------------------------
SAMMANFATTNING (Auto):
E-post från {message['from']} med ämne "{message['subject']}".
================================================================================

{message['body']}
"""
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    LOGGER.info(f"Sparad: {filename}")
    
    return True


# --- MAIN ---

def run_collector():
    """
    Huvudloop för Gmail Collector.
    """
    os.makedirs(MAIL_FOLDER, exist_ok=True)
    
    service = get_gmail_service()
    if not service:
        return 0
    
    # Hämta label ID
    label_id = get_label_id(service, TARGET_LABEL)
    if not label_id:
        LOGGER.warning(f"Label '{TARGET_LABEL}' finns inte - skapa den i Gmail")
        terminal_status("gmail", "Gmail", "skipped")
        return 0
    
    LOGGER.info(f"Gmail Collector startar: label={TARGET_LABEL}")

    # Hämta redan sparade message IDs för deduplicering
    existing_ids = get_existing_message_ids()
    LOGGER.info(f"Befintliga e-post: {len(existing_ids)}")
    
    new_messages = 0

    try:
        # Lista ALLA meddelanden med label (ingen datumbegränsning)
        page_token = None
        while True:
            results = service.users().messages().list(
                userId='me',
                labelIds=[label_id],
                pageToken=page_token
            ).execute()
            
            messages = results.get('messages', [])
            
            for msg in messages:
                msg_id = msg['id']
                
                # Deduplicering
                if msg_id in existing_ids:
                    continue
                
                # Hämta detaljer och spara
                try:
                    details = fetch_message_details(service, msg_id)
                    if details:
                        create_email_file(details)
                        new_messages += 1
                        existing_ids.add(msg_id)
                except Exception as e:
                    LOGGER.error(f"HARDFAIL: Kunde inte spara meddelande {msg_id}: {e}")
                    raise
            
            page_token = results.get('nextPageToken')
            if not page_token:
                break
            
            time.sleep(0.5)  # Rate limiting
            
    except Exception as e:
        LOGGER.error(f"HARDFAIL: Gmail Collector fel: {e}")
        raise
    
    LOGGER.info(f"Gmail Collector klar: {new_messages} nya e-post")
    total_count = len(existing_ids)

    # Bara visa status om det finns nya mail (tyst annars)
    if new_messages > 0:
        terminal_status("gmail", "Gmail", "done", detail=f"{new_messages} nya")

    return new_messages


if __name__ == "__main__":
    try:
        run_collector()
    except KeyboardInterrupt:
        LOGGER.info("Gmail Collector avslutad av användare")
    except Exception as e:
        LOGGER.error(f"HARDFAIL: {e}")
        terminal_status("gmail", "Gmail", "failed", str(e))
        raise




