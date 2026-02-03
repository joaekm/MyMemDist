# MyMemory Apps

Förbyggda appar för distribution.

## MyMemory.app

Native macOS menubar-app för status och inställningar.

### Installation (manuell)

1. **Ladda ner** `MyMemory.app.zip` eller kopiera `MyMemory.app`
2. **Packa upp** och flytta till `/Applications/`
3. **Första start:** Högerklicka → "Öppna" (krävs för osignerade appar)

Om macOS blockerar appen:
- Gå till **Systeminställningar → Integritet och säkerhet**
- Scrolla ner och klicka **"Öppna ändå"** bredvid MyMemory

### Funktioner

- Status-ikon i menyraden (Ⓜ)
- Visa status för alla tjänster
- Redigera config utan att öppna YAML-filer
- Progress-visning under rebuild

### Bygg från källkod

```bash
# Via Swift Package Manager (snabbast)
cd desktop/MyMemoryApp && swift build

# Via Xcode (full build)
./tools/build_menubar_app.sh --release
```

Kräver Xcode 15+ och macOS 14+. Källkoden finns i `desktop/MyMemoryApp/`.

### Distribution

Byggs automatiskt till denna mapp. Synkas till MyMemDist via:

```bash
python3 tools/sync_to_dist.py --confirm
```

### Autostart

För att starta appen automatiskt vid inloggning:
1. Öppna **Systeminställningar → Allmänt → Startobjekt**
2. Klicka **+** och välj MyMemory.app

Eller via Terminal:
```bash
osascript -e 'tell application "System Events" to make login item at end with properties {path:"/Applications/MyMemory.app", hidden:false}'
```
