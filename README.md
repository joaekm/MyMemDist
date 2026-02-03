# MyMemory Distribution

Ditt personliga AI-minne - samlar in, organiserar och gör all din information sökbar.

## Installation

1. **Ladda ner:** [MyMemory-Installer.dmg](apps/MyMemory-Installer.dmg)
2. **Öppna DMG:en** och dra `MyMemory.app` till Applications
3. **Starta appen** - högerklicka → "Öppna" (första gången)
4. **Setup Wizard** guidar dig genom resten

### Du behöver

- **Anthropic API-nyckel:** https://console.anthropic.com/settings/keys
- **Google Gemini API-nyckel:** https://aistudio.google.com/apikey

## Vad händer vid installation?

Setup Wizard i appen:
- Laddar ner backend-kod automatiskt
- Installerar Python 3.12 (lokalt, påverkar inte ditt system)
- Skapar virtual environment med dependencies
- Konfigurerar Claude Desktop MCP-integration
- Kopierar användarmanual för indexering

## Efter installation

- **Statusikon** visas i menyraden
- **Droppa filer** i `~/MyMemory/MemoryDrop/` för indexering
- **Fråga Claude** om din data via Claude Desktop

## Dokumentation

- [Användarmanual](docs/MyMemory_Användarmanual.md)

## Systemkrav

- macOS 14.0 eller senare
- Internetanslutning (för API-anrop)
- ~500 MB diskutrymme

---

*MyMemory - Ditt personliga AI-minne*
