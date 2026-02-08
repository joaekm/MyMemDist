import os
import time
import yaml
import logging
import datetime
import uuid
import zoneinfo
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# --- CONFIG ---
from services.utils.config_loader import get_config

CONFIG = get_config()

TZ_NAME = CONFIG.get('system', {}).get('timezone', 'UTC')
try:
    SYSTEM_TZ = zoneinfo.ZoneInfo(TZ_NAME)
except Exception as e:
    print(f"[CRITICAL] HARDFAIL: Ogiltig timezone '{TZ_NAME}': {e}")
    exit(1)

SLACK_FOLDER = os.path.expanduser(CONFIG['paths']['asset_slack'])
LOG_FILE = os.path.expanduser(CONFIG['logging'].get('system_log', '~/MyMemory/Logs/system.log'))
SLACK_CONF = CONFIG.get('slack', {})
BOT_TOKEN = SLACK_CONF.get('bot_token')
CHANNELS = SLACK_CONF.get('channels', [])
SLACK_PAGE_SIZE = CONFIG.get('collectors', {}).get('slack', {}).get('page_size', 200)
HISTORY_DAYS = SLACK_CONF.get('history_days', 7)

log_dir = os.path.dirname(LOG_FILE)
os.makedirs(log_dir, exist_ok=True)

# Configure root logger: file only, no console
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(logging.Formatter('%(asctime)s - SLACK - %(levelname)s - %(message)s'))
root_logger.addHandler(file_handler)

LOGGER = logging.getLogger('MyMem_SlackArchiver')

# Terminal status
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from services.utils.terminal_status import service_status

# Guard: avsluta rent om Slack inte är konfigurerat
if not BOT_TOKEN:
    LOGGER.info("Slack bot_token saknas i config — Slack Collector inaktiv")
    service_status("Slack Collector", "skipped")
    exit(0)

if not CHANNELS:
    LOGGER.info("Inga Slack-kanaler konfigurerade — Slack Collector inaktiv")
    service_status("Slack Collector", "skipped")
    exit(0)

CLIENT = WebClient(token=BOT_TOKEN)
USER_CACHE = {}

def get_user_name(user_id):
    if user_id in USER_CACHE: return USER_CACHE[user_id]
    try:
        result = CLIENT.users_info(user=user_id)
        user = result["user"]
        name = user.get("real_name") or user.get("profile", {}).get("display_name") or user.get("name")
        USER_CACHE[user_id] = name
        return name
    except: return user_id

def get_channel_name(channel_id):
    try:
        res = CLIENT.conversations_info(channel=channel_id)
        return res["channel"]["name"]
    except: return channel_id

def format_slack_time(ts):
    try:
        dt = datetime.datetime.fromtimestamp(float(ts), SYSTEM_TZ)
        return dt.strftime('%H:%M')
    except: return "??:??"

def fetch_daily_messages(channel_id, target_date):
    local_start = datetime.datetime.combine(target_date, datetime.time.min).replace(tzinfo=SYSTEM_TZ)
    local_end = datetime.datetime.combine(target_date, datetime.time.max).replace(tzinfo=SYSTEM_TZ)
    
    start_ts = local_start.timestamp()
    end_ts = local_end.timestamp()
    
    all_messages = []
    try:
        cursor = None
        while True:
            result = CLIENT.conversations_history(channel=channel_id, oldest=start_ts, latest=end_ts, limit=SLACK_PAGE_SIZE, cursor=cursor)
            messages = result["messages"]
            all_messages.extend(messages)
            if not result.get("has_more"): break
            cursor = result.get("response_metadata", {}).get("next_cursor")
            time.sleep(0.5) 
        all_messages.sort(key=lambda x: float(x['ts']))
        return all_messages
    except SlackApiError as e:
        LOGGER.error(f"API Fel ({channel_id}): {e}")
        return []

def fetch_replies(channel_id, thread_ts):
    try:
        result = CLIENT.conversations_replies(channel=channel_id, ts=thread_ts)
        return result["messages"][1:] if len(result["messages"]) > 0 else []
    except Exception as e:
        LOGGER.error(f"HARDFAIL: Kunde inte hämta trådsvar för {thread_ts}: {e}")
        raise RuntimeError(f"HARDFAIL: Kunde inte hämta trådsvar") from e

def archive_day(channel_id, target_date):
    date_str = target_date.strftime('%Y-%m-%d')
    ch_name = get_channel_name(channel_id)
    
    existing_files = os.listdir(SLACK_FOLDER)
    base_pattern = f"Slack_{ch_name}_{date_str}_"
    for f in existing_files:
        if f.startswith(base_pattern) and f.endswith(".txt"): return False 

    messages = fetch_daily_messages(channel_id, target_date)
    if not messages: return False 

    LOGGER.info(f"Arkiverar {date_str} för #{ch_name}")

    full_transcript = []
    participants = set()

    for msg in messages:
        user = get_user_name(msg.get("user"))
        participants.add(user)
        time_str = format_slack_time(msg['ts'])
        full_transcript.append(f"[{time_str}] {user}: {msg.get('text', '')}")
        
        if msg.get("thread_ts") and msg.get("reply_count", 0) > 0:
            replies = fetch_replies(channel_id, msg["thread_ts"])
            for r in replies:
                r_user = get_user_name(r.get("user"))
                participants.add(r_user)
                r_time = format_slack_time(r['ts'])
                full_transcript.append(f"    ↳ [{r_time}] {r_user}: {r.get('text', '')}")

    unit_id = str(uuid.uuid4())
    filnamn = f"Slack_{ch_name}_{date_str}_{unit_id}.txt" 
    ut_sokvag = os.path.join(SLACK_FOLDER, filnamn)
    participants_list = "\n- ".join(sorted(list(participants)))
    content = "\n".join(full_transcript)
    
    archived_at = datetime.datetime.now(SYSTEM_TZ).isoformat()
    
    # SKAPA DATUM_TID (Mitt på dagen för att vara tydlig)
    # Vi konverterar dagens datum till en ISO-sträng kl 12:00 i lokal tidszon.
    # Detta gör formatet kompatibelt med Transcriberns ISO-format.
    day_iso = datetime.datetime.combine(target_date, datetime.time(12, 0)).replace(tzinfo=SYSTEM_TZ).isoformat()

    # HÄR ÄR ÄNDRINGEN: DATUM_TID
    header = f"""================================================================================
METADATA FRÅN SLACK (MyMem Daily Digest)
================================================================================
KANAL:         #{ch_name}
DATUM_TID:     {day_iso}
ARKIVERAD:     {archived_at}
KÄLLA:         Slack API
UNIT_ID:       {unit_id}
--------------------------------------------------------------------------------
DELTAGARE:
- {participants_list}

SAMMANFATTNING (Auto):
Daglig logg från Slack-kanalen #{ch_name} för datumet {date_str}.
Innehåller {len(messages)} huvuddiskussioner.
================================================================================

{content}
"""
    with open(ut_sokvag, 'w', encoding='utf-8') as f: f.write(header)
    LOGGER.info(f"Sparad: {filnamn}")
    return True

if __name__ == "__main__":
    os.makedirs(SLACK_FOLDER, exist_ok=True)
    POLL_INTERVAL = 3600

    service_status("Slack Collector", "started")
    
    try:
        while True:
            today = datetime.date.today()
            for i in range(1, HISTORY_DAYS + 1):
                target_date = today - datetime.timedelta(days=i)
                for channel in CHANNELS:
                    try: 
                        archive_day(channel, target_date)
                    except Exception as e:
                        LOGGER.error(f"HARDFAIL: Arkivering misslyckades för {channel} {target_date}: {e}")
                        raise
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        LOGGER.info("Slack Collector avslutad av användare")