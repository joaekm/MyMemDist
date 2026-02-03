# MyMemory Användarmanual

> **Version 2.0** | Februari 2026 | Alfa-status

MyMemory är ett lokalt "företagsminne" som samlar, organiserar och gör all din information sökbar via AI. Systemet exponeras som en MCP-server, vilket innebär att du kan använda det direkt i Claude Desktop, Cursor eller andra MCP-kompatibla verktyg — utan att behöva underhålla en egen chattapplikation.

---

## Innehåll

1. [Vad är MyMemory?](#vad-är-mymemory)
2. [Kom igång](#kom-igång)
3. [Mappar och dataflöde](#mappar-och-dataflöde)
4. [Lägga till data](#lägga-till-data)
5. [Söka och fråga](#söka-och-fråga)
6. [MCP-verktyg — referens](#mcp-verktyg--referens)
7. [Mötesbevakning i realtid](#mötesbevakning-i-realtid)
8. [Menubar-appen](#menubar-appen)
9. [Integrationer](#integrationer)
10. [Felsökning](#felsökning)
11. [Vanliga frågor](#vanliga-frågor)

---

## Vad är MyMemory?

MyMemory fungerar som en "andra hjärna" — ett system som:

- **Samlar in** dokument, ljudinspelningar, e-post, Slack-meddelanden och kalenderhändelser
- **Organiserar** allt i en sökbar kunskapsgraf med entiteter (personer, projekt, organisationer) och relationer
- **Exponerar** kunskapen via 14 MCP-verktyg som din AI-assistent kan använda

### Kärnkoncept

| Begrepp | Beskrivning |
|---------|-------------|
| **Assets** | Dina originalfiler (PDF, ljud, etc.) — bevaras orörda |
| **Lake** | Normaliserade markdown-filer med metadata — mellanlagret |
| **Index** | Vektordatabas (ChromaDB) + kunskapsgraf (DuckDB) — "hjärnan" |
| **MCP** | Model Context Protocol — låter AI-verktyg anropa MyMemory-funktioner |

### Vad MyMemory *inte* är

MyMemory är **händerna**, inte hjärnan. Systemet lagrar, strukturerar och söker — men all "reasoning" sker i Claude eller annat AI-verktyg som du använder. Du får bästa resultat genom att ställa frågor naturligt och låta Claude använda verktygen intelligent.

---

## Kom igång

### Systemkrav

- macOS 12+ (Monterey eller senare)
- Python 3.12
- 8 GB RAM (16 GB rekommenderas för större kunskapsbaser)
- ffmpeg (för ljudtranskribering): `brew install ffmpeg`

### Installation

1. **Öppna MyMemory.app** från Applications
2. **Första gången:** Setup Wizard startar automatiskt
   - Fyll i ditt namn
   - Klistra in API-nycklar (se nedan)
   - Vänta medan backend installeras
3. **Klart!** Ikonen visas i menyraden

### API-nycklar

Du behöver två nycklar:

**Anthropic Claude** (för textanalys, entitetsextraktion, förädling)
1. Gå till [console.anthropic.com/settings/keys](https://console.anthropic.com/settings/keys)
2. Skapa en ny API-nyckel
3. Kopiera nyckeln (börjar med `sk-ant-`)

**Google Gemini** (för ljudtranskribering)
1. Gå till [aistudio.google.com/apikey](https://aistudio.google.com/apikey)
2. Klicka "Create API Key"
3. Kopiera nyckeln

### Konfigurera Claude Desktop

För att Claude Desktop ska kunna använda MyMemory, lägg till detta i din MCP-konfiguration (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "mymem": {
      "command": "python",
      "args": ["/path/to/MyMemory/services/agents/mymem_mcp.py"],
      "env": {}
    }
  }
}
```

Starta om Claude Desktop efter ändringen.

---

## Mappar och dataflöde

```
~/MyMemory/
├── Assets/                 # LAGRING 1: Originalfiler (rör aldrig)
│   ├── Documents/          # PDF, Word, text
│   ├── Recordings/         # Ljudfiler att transkribera
│   ├── Transcripts/        # Transkriberade texter (skapas automatiskt)
│   ├── Slack/              # Slack-exporter
│   ├── Mail/               # E-post
│   └── Calendar/           # Kalenderhändelser
│
├── Lake/                   # LAGRING 2: Normaliserad markdown (rör aldrig)
│
├── Index/                  # LAGRING 3: Databaser (rör aldrig)
│   ├── VectorDB/           # ChromaDB — semantisk sökning
│   └── GraphDB/            # DuckDB — kunskapsgraf
│
├── MemoryDrop/             # DROPZON — lägg filer här för snabb indexering
│
├── Logs/                   # Systemloggar
│   └── system.log
│
└── Config/                 # Konfiguration
    └── my_mem_config.yaml
```

### Dataflödet i tre faser

```
┌─────────────────────────────────────────────────────────────────┐
│ FAS 1: INSAMLING                                                │
│ MemoryDrop → Assets (filer får UUID-namnstandard)               │
│ Slack/Gmail/Calendar → Assets (via collectors)                  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ FAS 2: INGESTION (per dokument, automatiskt)                    │
│ • Textextraktion                                                │
│ • Entitetsextraktion (personer, organisationer, projekt)        │
│ • Grafkopplingar (vem nämndes? vilka relationer?)               │
│ • Vektorindexering (semantisk sökning)                          │
│ → Output: Lake-fil + Graf-noder + Vektor                        │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ FAS 3: FÖRÄDLING (batch, schemalagt)                            │
│ Dreamer analyserar hela kunskapsbasen:                          │
│ • Slår ihop dubbletter ("J.S." + "Jan Ström" → samma person)    │
│ • Uppdaterar relationer och kontext                             │
│ • Förbättrar sökbarhet                                          │
└─────────────────────────────────────────────────────────────────┘
```

---

## Lägga till data

### Metod 1: MemoryDrop (snabbast)

1. Öppna Finder
2. Gå till `~/MyMemory/MemoryDrop/`
3. Dra och släpp filer
4. Filerna indexeras automatiskt inom sekunder

**Tips:** Lägg till MemoryDrop som favorit i Finder för snabb åtkomst:
1. Navigera till `~/MyMemory/`
2. Dra `MemoryDrop`-mappen till **Favoriter** i Finder-sidofältet
3. Nu kan du släppa filer direkt i favoriten från var som helst

**Stödda format:**
- Dokument: `.pdf`, `.docx`, `.txt`, `.md`
- Ljud: `.m4a`, `.mp3`, `.wav`, `.webm`
- Bilder med text: `.png`, `.jpg` (OCR extraherar text)

### Metod 2: Direkt till Assets

Lägg filer direkt i rätt Assets-undermapp:
- `Assets/Documents/` — för dokument
- `Assets/Recordings/` — för ljudfiler (transkriberas automatiskt)

### Metod 3: Via Claude (ingest_content)

Du kan be Claude spara information direkt till minnet:

> "Spara följande mötesanteckningar i MyMemory: [anteckningar]"

Claude använder då verktyget `ingest_content` för att skapa ett nytt dokument.

### Ljudtranskribering

Ljudfiler i `Assets/Recordings/` transkriberas automatiskt via en 8-stegs pipeline:

1. **Metadata** — duration, format analyseras
2. **Chunking** — långa filer delas i 5 MB-delar
3. **Transkribering** — Gemini transkriberar parallellt
4. **Kalendermatch** — hittar relaterat möte ±30 min
5. **Kontextberikning** — söker relaterade entiteter i grafen
6. **Speaker-mapping** — mappar röster till kända personer
7. **Strukturering** — skapar rubriker och delar
8. **Indexering** — sparar till Lake + Graf + Vektor

**Tips:** Spela in möten med telefonen och lägg filen i Recordings-mappen efteråt.

---

## Söka och fråga

### Grundprincip: Fråga naturligt

MyMemory exponerar 14 verktyg via MCP. Du behöver inte veta vilka — fråga bara naturligt så väljer Claude rätt verktyg:

| Du frågar | Claude använder |
|-----------|-----------------|
| "Vad pratade vi om på mötet förra veckan?" | `parse_relative_date` → `search_by_date_range` → `read_document_content` |
| "Berätta allt du vet om vår kontaktperson på Acme AB" | `search_graph_nodes` → `get_entity_summary` → `get_neighbor_network` |
| "Hitta dokument om upphandlingen" | `query_vector_memory` |
| "Vilka projekt är konsulten inblandad i?" | `search_graph_nodes` → `get_neighbor_network` |
| "Sammanfatta alla möten från januari" | `search_by_date_range` → `read_document_content` (flera) |

### Exempelfrågor

**Personbaserat:**
- "Vad vet du om vår kontaktperson på leverantören?"
- "Vilka har jag haft möten med den senaste månaden?"
- "Vad diskuterade jag med projektledaren senast?"

**Projektbaserat:**
- "Sammanfatta projekt X"
- "Vilka beslut har fattats om budgeten?"
- "Vilka är inblandade i upphandlingen?"

**Tidsbaserat:**
- "Vad hände igår?"
- "Visa alla dokument från Q4 2025"
- "Vad diskuterades på mötet den 15 januari?"

**Sökning:**
- "Hitta alla dokument som nämner AI-strategi"
- "Sök efter e-post från Google"
- "Vilka Slack-meddelanden handlar om deadline?"

---

## MCP-verktyg — referens

### Sökning (10 verktyg)

| Verktyg | Användning | Exempel |
|---------|------------|---------|
| `search_graph_nodes` | Hitta specifika entiteter via namn | "Finns noden Acme AB?" |
| `query_vector_memory` | Semantisk sökning på koncept | "Vem jobbar med AI-projekt?" |
| `search_by_date_range` | Tidsfiltrera dokument | "Vad hände 2025-12-01 till 2025-12-31?" |
| `search_lake_metadata` | Sök i YAML-metadata | "Alla dokument med source=slack" |
| `get_neighbor_network` | Kartlägg relationer | "Vilka är kopplade till projekt X?" |
| `get_entity_summary` | Djupdyk i en entitet | "Berätta allt om denna person" |
| `get_graph_statistics` | Grafstatistik | "Hur många noder finns?" |
| `parse_relative_date` | Översätt tidsuttryck | "förra veckan" → datum |
| `read_document_content` | Läs källdokument | Hämta originaltext |
| `ingest_content` | Skapa nytt dokument | Spara mötesanteckningar |

### Skillnad: search_graph_nodes vs query_vector_memory

| Aspekt | search_graph_nodes | query_vector_memory |
|--------|-------------------|---------------------|
| **Frågar** | "Finns noden X?" | "Vem/vad handlar om Y?" |
| **Söker i** | Namn, alias, ID | Semantisk kontext |
| **Bäst för** | Kända namn | Öppna frågor |
| **Exempel** | "Acme AB" | "budget-diskussioner" |

### Mötesbevakning (4 verktyg)

| Verktyg | Användning |
|---------|------------|
| `watch_meeting` | Starta/stoppa/kolla mötesbevakning |
| `add_test_chunk` | Lägg till testdata (debugging) |
| `get_buffer_status` | Visa bufferstatus |
| `clear_buffer` | Rensa buffern |

---

## Mötesbevakning i realtid

MyMemory kan bevaka möten i realtid och ge dig "whispers" — diskreta tips baserade på vad som sägs.

### Starta bevakning

Säg till Claude:
> "Starta mötesbevakning" eller "Bevaka mötet"

Claude anropar `watch_meeting(action="start")` och börjar lyssna på nya transkript-chunks.

### Under mötet

När någon nämner något relevant (t.ex. en person eller ett projekt), söker Claude automatiskt i minnet och kan ge dig bakgrund:

> **Whisper:** "Någon nämnde 'Adda Inköpscentral'. Enligt minnet är detta en organisation ni hade kontakt med i november angående upphandling."

### Avsluta bevakning

> "Stoppa mötesbevakning"

---

## Menubar-appen

### Statusikoner

| Ikon | Betydelse |
|------|-----------|
| ⓜ | Allt fungerar normalt |
| ✕ | Tjänster stoppade |
| ! | Fel har uppstått |
| ? | Okänd status |

### Funktioner

**Klicka på ikonen:**
- Se status för alla tjänster (Ingestion, Transcriber, Dreamer, Collectors)
- Snabbinställningar (aktivera/avaktivera Dreamer, ändra log level)
- Progress vid rebuild

**Inställningar (kugghjulet):**
- Ändra API-nycklar
- Konfigurera collectors (Slack, Gmail, Calendar)
- Justera thresholds

---

## Integrationer

### Slack

1. Skapa en Slack-app på [api.slack.com/apps](https://api.slack.com/apps)
2. Lägg till OAuth scopes: `channels:history`, `channels:read`, `users:read`
3. Installera appen i din workspace
4. Kopiera Bot Token (börjar med `xoxb-`)
5. Lägg till token i MyMemory-inställningar
6. Ange vilka kanaler som ska bevakas

**Insamlingsintervall:** Var 60:e minut

### Gmail

1. Gå till [Google Cloud Console](https://console.cloud.google.com/)
2. Skapa ett projekt
3. Aktivera Gmail API
4. Skapa OAuth 2.0 credentials (Desktop app)
5. Ladda ner `client_secret.json`
6. Lägg filen i `~/MyMemory/Config/Credentials/`
7. Skapa en Gmail-label (t.ex. "MyMemory")
8. E-post med denna label indexeras automatiskt

**Insamlingsintervall:** Var 5:e minut

### Google Calendar

1. Samma credentials som Gmail
2. Aktivera Calendar API i Google Cloud Console
3. Kalenderhändelser indexeras automatiskt

**Insamlingsintervall:** Var 5:e minut

---

## Felsökning

### Appen startar inte

**macOS blockerar appen:**
1. Systeminställningar → Integritet och säkerhet
2. Scrolla till "MyMemory blockerades"
3. Klicka "Öppna ändå"

**Första gången:**
- Högerklicka → "Öppna" → "Öppna" i dialogrutan

### Filer indexeras inte

1. **Kontrollera mappen:** Ligger filen i `MemoryDrop/` eller `Assets/Documents/`?
2. **Kolla loggen:** `tail -100 ~/MyMemory/Logs/system.log`
3. **Verifiera tjänster:** Grön status i menubar?
4. **UUID-format:** Filer behöver UUID-suffix (skapas automatiskt via MemoryDrop)

### Sökning ger inga resultat

1. **Vänta:** Nya filer behöver några sekunder för indexering
2. **Starta om Claude Desktop:** Behövs ibland efter installation
3. **Testa enklare sökning:** "Vilka dokument finns?" för att verifiera koppling
4. **Kontrollera MCP-status:** Säg "Visa grafstatistik" för att verifiera att data finns

### Transkribering fungerar inte

1. **ffmpeg installerat?** `brew install ffmpeg`
2. **Gemini-nyckel giltig?** Testa på [aistudio.google.com](https://aistudio.google.com)
3. **Filformat stöds?** `.m4a`, `.mp3`, `.wav`, `.webm`
4. **Kolla loggen:** `grep -i "transcrib" ~/MyMemory/Logs/system.log | tail -20`

### Dreamer körs inte

Dreamer triggas av en threshold (antal nya dokument sedan senaste körning). Om du vill köra manuellt:

```bash
python ~/MyMemory/services/engines/dreamer.py --force
```

### Loggkommandon

```bash
# Visa senaste loggarna
tail -100 ~/MyMemory/Logs/system.log

# Sök efter fel
grep -i "error" ~/MyMemory/Logs/system.log | tail -20

# Följ loggen i realtid
tail -f ~/MyMemory/Logs/system.log

# Sök efter specifik fil
grep "filnamn" ~/MyMemory/Logs/system.log
```

---

## Vanliga frågor

### Hur mycket kostar det?

MyMemory är gratis. Du betalar för API-användning:
- **Anthropic Claude:** ca $3–15/månad beroende på användning
- **Google Gemini:** Gratis tier räcker oftast

### Var lagras min data?

All data lagras **lokalt** i `~/MyMemory/`. Inget skickas till molnet permanent.

**Vad skickas till API:er:**
- Text skickas till Anthropic för analys (inte lagras)
- Ljud skickas till Gemini för transkribering (inte lagras)

### Kan jag använda MyMemory offline?

Nej, AI-funktionerna kräver internetanslutning. Men all din data finns lokalt och försvinner inte vid avbrott.

### Hur tar jag backup?

```bash
# Skapa backup
cp -r ~/MyMemory ~/MyMemory_backup_$(date +%Y%m%d)
```

**Viktigast att säkerhetskopiera:**
- `Assets/` — dina originalfiler
- `Lake/` — processade dokument
- `Index/` — databaserna

### Hur återställer jag från backup?

Använd verktyget för staged rebuild:

```bash
# Terminal 1: Starta tjänster
python ~/MyMemory/start_services.py

# Terminal 2: Kör rebuild
python ~/MyMemory/tools/tool_staged_rebuild.py
```

Rebuild-verktyget:
- Frågar vilken backup du vill använda
- Rensar nuvarande data
- Återställer kronologiskt (äldst först)
- Kan avbrytas och återupptas

### Hur avinstallerar jag?

1. Ta bort `/Applications/MyMemory.app`
2. Ta bort `~/MyMemory/` (OBS: raderar all data)
3. Ta bort MCP-konfiguration i Claude Desktop

### Kan flera personer dela en kunskapsbas?

Just nu är MyMemory designat för personlig användning. Delad kunskapsbas är planerad för framtida versioner (se arkitekturdokumentet för "Privacy-First"-principen och åtkomstnivåer).

### Hur hanteras dubbletter?

Dreamer-komponenten körs periodiskt och:
- Identifierar dubbletter (t.ex. "J.S." vs "Jan Ström")
- Slår ihop noder med samma identitet
- Uppdaterar relationer

### Vad är skillnaden mellan Lake och Index?

| Aspekt | Lake | Index |
|--------|------|-------|
| Format | Markdown + YAML | Vektorer + Graf |
| Syfte | Mänskligt läsbar, portabel | AI-optimerad sökning |
| Kan redigeras | Ja (men rekommenderas inte) | Nej |
| Vid flytt | Kopiera | Byggs om |

---

## Teknisk referens

### 3-Timestamp-systemet

Varje dokument i Lake har tre tidsstämplar:

| Fält | Betydelse |
|------|-----------|
| `timestamp_ingestion` | När filen indexerades |
| `timestamp_content` | När innehållet faktiskt hände |
| `timestamp_updated` | Senast uppdaterad av Dreamer |

### Tech stack

| Komponent | Teknologi |
|-----------|-----------|
| Språk | Python 3.12 |
| Vektordatabas | ChromaDB |
| Grafdatabas | DuckDB |
| AI (text) | Anthropic Claude (Sonnet/Haiku) |
| AI (ljud) | Google Gemini Flash |
| Embeddings | KBLab/sentence-bert-swedish-cased (768 dim) |
| MCP | FastMCP |

### Konfigurationsfiler

| Fil | Syfte |
|-----|-------|
| `config/my_mem_config.yaml` | Sökvägar, API-nycklar, modeller |
| `config/graph_schema_template.json` | Nodtyper, relationer (SSOT) |
| `config/lake_metadata_template.json` | Lake frontmatter-schema |
| `config/services_prompts.yaml` | Promptar för AI-tjänster |

---

## Kontakt och support

**GitHub:** [github.com/joaekm/MyMemDist](https://github.com/joaekm/MyMemDist)

**Vid problem:**
1. Kolla denna manual
2. Fråga Claude: "Hur löser jag [problem] i MyMemory?"
3. Sök i loggarna: `grep -i "error" ~/MyMemory/Logs/system.log`
4. Skapa en issue på GitHub

---

*MyMemory — Ditt personliga AI-minne*  
*Version 2.0 | Alfa | Februari 2026*
