# MyMemory Användarmanual

## Vad är MyMemory?

MyMemory är ditt personliga AI-minne - ett system som samlar in, organiserar och gör all din information sökbar. Tänk på det som en "andra hjärna" som kommer ihåg allt åt dig.

### Huvudfunktioner

- **Dokumentindexering** - PDF, Word, textfiler indexeras automatiskt
- **Ljudtranskribering** - Mötesinspelningar och röstmeddelanden blir sökbar text
- **Slack-integration** - Meddelanden från utvalda kanaler samlas in
- **Gmail-integration** - E-post med specifik label indexeras
- **Kalender-integration** - Möten och händelser sparas
- **AI-sökning** - Fråga naturligt och få relevanta svar från all din data

---

## Kom igång

### Installation

1. Öppna MyMemory.app från Applications
2. Första gången visas Setup Wizard automatiskt
3. Fyll i ditt namn och API-nycklar
4. Vänta medan systemet installerar backend
5. Klart! Ikonen visas i menyraden

### API-nycklar

Du behöver två API-nycklar:

**Anthropic (Claude)**
- Gå till: https://console.anthropic.com/settings/keys
- Skapa en ny API-nyckel
- Kopitera nyckeln (börjar med `sk-ant-`)

**Google Gemini**
- Gå till: https://aistudio.google.com/apikey
- Klicka "Create API Key"
- Kopiera nyckeln

---

## Använda MyMemory

### Lägga till dokument

**Metod 1: MemoryDrop**
1. Öppna Finder
2. Gå till `~/MyMemory/MemoryDrop/`
3. Dra och släpp filer hit
4. Filerna indexeras automatiskt inom några sekunder

**Metod 2: Direkt till Assets**
1. Gå till `~/MyMemory/Assets/Documents/`
2. Lägg filer här
3. Ingestion Engine plockar upp dem automatiskt

**Stödda format:**
- PDF (.pdf)
- Word (.docx)
- Text (.txt, .md)
- Bilder med text (.png, .jpg) - OCR extraherar text

### Transkribera ljud

1. Lägg ljudfiler i `~/MyMemory/Assets/Recordings/`
2. Stödda format: .m4a, .mp3, .wav, .webm
3. Transcriber konverterar automatiskt till text
4. Transkriptionen sparas i `~/MyMemory/Assets/Transcripts/`
5. Texten indexeras sedan som vanligt dokument

**Tips:** Spela in möten med din telefon eller dator och lägg filen i Recordings-mappen efteråt.

### Söka i MyMemory

**Via Claude Desktop:**
1. Öppna Claude Desktop
2. MyMemory visas som en MCP-server (tools)
3. Fråga naturligt: "Vad pratade vi om på mötet förra veckan?"
4. Claude söker i din data och svarar

**Exempel på frågor:**
- "Vad sa Anna om budgeten?"
- "Hitta alla dokument om projekt X"
- "Sammanfatta mina möten från januari"
- "Vem nämnde deadline för leveransen?"

---

## Mappar och struktur

```
~/MyMemory/
├── Assets/                 # Originalfiler (rör ej)
│   ├── Documents/          # PDF, Word, text
│   ├── Recordings/         # Ljudfiler att transkribera
│   ├── Transcripts/        # Transkriberade texter
│   ├── Slack/              # Slack-exporter
│   ├── Mail/               # E-post
│   └── Calendar/           # Kalenderhändelser
├── Lake/                   # Processade .md-filer (rör ej)
├── Index/                  # Databaser (rör ej)
│   ├── VectorDB/           # Semantisk sökning
│   └── GraphDB/            # Kunskapsgraf
├── MemoryDrop/             # LÄGG FILER HÄR för snabb indexering
├── Logs/                   # Systemloggar
└── Runtime/                # Backend-kod (rör ej)
```

**Viktigt:** Rör aldrig filerna i Lake/, Index/ eller Runtime/. Dessa hanteras av systemet.

---

## Menubar-appen

### Statusikoner

- **ⓜ** (m.circle) - Allt fungerar normalt
- **✕** (x.circle) - Tjänster stoppade
- **!** (exclamationmark.circle) - Fel har uppstått
- **?** (questionmark.circle) - Okänd status

### Funktioner

**Klicka på ikonen för att se:**
- Status för alla tjänster
- Snabbinställningar (Dreamer, Log Level)
- Progress om rebuild pågår

**Inställningar:**
- Klicka kugghjulet för fullständiga inställningar
- Här kan du ändra alla konfigurationer

---

## Integrationer

### Slack

1. Skapa en Slack-app på https://api.slack.com/apps
2. Lägg till OAuth scopes: `channels:history`, `channels:read`, `users:read`
3. Installera appen i din workspace
4. Kopiera Bot Token (börjar med `xoxb-`)
5. Lägg till token i MyMemory-inställningar
6. Ange vilka kanaler som ska bevakas

### Gmail

1. Gå till Google Cloud Console
2. Skapa ett projekt
3. Aktivera Gmail API
4. Skapa OAuth 2.0 credentials (Desktop app)
5. Ladda ner `client_secret.json`
6. Lägg filen i `~/MyMemory/Credentials/`
7. Skapa en Gmail-label (t.ex. "MyMemory")
8. E-post med denna label indexeras automatiskt

### Google Calendar

1. Samma credentials som Gmail
2. Aktivera Calendar API i Google Cloud Console
3. Kalenderhändelser indexeras automatiskt

---

## Felsökning

### Appen startar inte

**macOS blockerar appen:**
1. Gå till Systeminställningar → Integritet och säkerhet
2. Scrolla ner till "MyMemory blockerades"
3. Klicka "Öppna ändå"

**Första gången:**
- Högerklicka på appen → "Öppna"
- Klicka "Öppna" i dialogrutan

### Filer indexeras inte

1. Kolla att filen ligger i rätt mapp (`MemoryDrop/` eller `Assets/Documents/`)
2. Kolla loggen: `~/MyMemory/Logs/system.log`
3. Kontrollera att Ingestion Engine körs (grön status i menubar)

### Sökning ger inga resultat

1. Vänta några sekunder efter att du lagt till filer
2. Kontrollera att Claude Desktop är omstartat efter installation
3. Testa med en enklare sökning först

### Transkribering fungerar inte

1. Kontrollera att ffmpeg är installerat: `brew install ffmpeg`
2. Kolla att Gemini API-nyckeln är giltig
3. Se loggen för felmeddelanden

### Loggar

Alla fel loggas till: `~/MyMemory/Logs/system.log`

```bash
# Visa senaste loggarna
tail -100 ~/MyMemory/Logs/system.log

# Sök efter fel
grep -i "error" ~/MyMemory/Logs/system.log | tail -20
```

---

## Vanliga frågor

### Hur mycket kostar det?

MyMemory är gratis, men du betalar för API-användning:
- **Anthropic Claude**: ca $3-15/månad beroende på användning
- **Google Gemini**: Gratis tier räcker oftast

### Var lagras min data?

All data lagras lokalt på din dator i `~/MyMemory/`. Inget skickas till molnet förutom:
- Text skickas till Anthropic/Gemini för AI-bearbetning
- Ingen data sparas permanent hos dessa tjänster

### Kan jag använda MyMemory offline?

Nej, AI-funktionerna kräver internetanslutning för API-anrop. Men all din data finns lokalt och försvinner inte om du tappar internet.

### Hur tar jag backup?

Kopiera hela `~/MyMemory/`-mappen. Viktigast är:
- `Assets/` - Dina originalfiler
- `Lake/` - Processade dokument
- `Index/` - Databaserna

### Hur avinstallerar jag?

1. Ta bort `/Applications/MyMemory.app`
2. Ta bort `~/MyMemory/` (varning: raderar all data)
3. Ta bort Claude Desktop MCP-konfiguration

---

## Kontakt och support

**GitHub:** https://github.com/joaekm/MyMemDist

**Problem?**
1. Kolla först denna manual
2. Fråga Claude: "Hur löser jag [problem] i MyMemory?"
3. Skapa en issue på GitHub

---

*MyMemory - Ditt personliga AI-minne*
