#!/usr/bin/env python3
"""
Terminal Status - Visuell feedback i terminalen.

Princip: Terminalen är "dashboard", loggen är "forensics".
- Endast start, klar, och kritiska fel visas i terminalen
- Allt annat går till loggfil

Användning:
    from services.utils.terminal_status import status, TerminalStatus

    status("ingestion", "Meeting_notes.pdf", "processing")
    status("ingestion", "Meeting_notes.pdf", "done")
    status("ingestion", "corrupt.pdf", "failed", "Schema validation error")
"""

import sys
import re
import threading

# ANSI färgkoder
COLORS = {
    "reset": "\033[0m",
    "bold": "\033[1m",
    "red": "\033[91m",
    "green": "\033[92m",
    "yellow": "\033[93m",
    "blue": "\033[94m",
    "magenta": "\033[95m",
    "cyan": "\033[96m",
    "white": "\033[97m",
}

# Process-konfiguration: emoji, färg vid processing
PROCESS_CONFIG = {
    "transcriber":    {"emoji": "🎧", "color": "cyan"},
    "ingestion":      {"emoji": "📄", "color": "blue"},
    "slack":          {"emoji": "💬", "color": "magenta"},
    "gmail":          {"emoji": "📧", "color": "magenta"},
    "calendar":       {"emoji": "📅", "color": "magenta"},
    "retriever":      {"emoji": "📥", "color": "yellow"},
    "rode":           {"emoji": "🎙️", "color": "cyan"},
    "rebuild":        {"emoji": "🔄", "color": "white"},
    "dreamer":        {"emoji": "💭", "color": "cyan"},
    "graph_health":   {"emoji": "🩺", "color": "green"},
    "service":        {"emoji": "⚙️", "color": "white"},
}

# Status-symboler
STATUS_SYMBOLS = {
    "processing": "⟳",
    "done": "✓",
    "failed": "✗",
    "waiting": "◦",
    "skipped": "−",
}

# UUID-mönster för att strippa från filnamn
UUID_PATTERN = re.compile(
    r'_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',
    re.IGNORECASE
)

# Thread-safe print lock
_print_lock = threading.Lock()


def strip_uuid(name: str) -> str:
    """Ta bort UUID från filnamn för läsbarhet."""
    return UUID_PATTERN.sub('', name)


def strip_extension(name: str) -> str:
    """Ta bort filändelse."""
    if '.' in name:
        return name.rsplit('.', 1)[0]
    return name


def clean_name(name: str) -> str:
    """Rensa filnamn för visning: ta bort UUID och extension."""
    name = strip_uuid(name)
    name = strip_extension(name)
    # Ta bort eventuella dubbla understreck
    name = re.sub(r'_+', '_', name)
    # Ta bort trailing underscore
    name = name.rstrip('_')
    return name


def colorize(text: str, color: str) -> str:
    """Lägg till ANSI-färgkod."""
    if color not in COLORS:
        return text
    return f"{COLORS[color]}{text}{COLORS['reset']}"


def status(
    process: str,
    item: str,
    state: str,
    error_msg: str = None,
    detail: str = None,
    *,
    show_raw_name: bool = False
) -> None:
    """
    Skriv statusrad till terminalen.

    Args:
        process: Processtyp (ingestion, transcriber, slack, etc.)
        item: Filnamn eller identifierare
        state: Status (processing, done, failed, waiting, skipped)
        error_msg: Felmeddelande vid failed
        detail: Extra info vid done, t.ex. "(3 noder, 5 relationer)"
        show_raw_name: Visa hela filnamnet utan att rensa UUID
    """
    config = PROCESS_CONFIG.get(process, {"emoji": "•", "color": "white"})
    emoji = config["emoji"]
    base_color = config["color"]

    # Rensa namn om inte show_raw_name
    display_name = item if show_raw_name else clean_name(item)

    # Välj symbol och färg baserat på state
    symbol = STATUS_SYMBOLS.get(state, "?")

    if state == "done":
        symbol_colored = colorize(symbol, "green")
        name_colored = colorize(display_name, "green")
    elif state == "failed":
        symbol_colored = colorize(symbol, "red")
        name_colored = colorize(display_name, "red")
    elif state == "processing":
        symbol_colored = colorize(symbol, base_color)
        name_colored = colorize(display_name, base_color)
    elif state == "skipped":
        symbol_colored = colorize(symbol, "yellow")
        name_colored = colorize(display_name, "yellow")
    else:
        symbol_colored = symbol
        name_colored = display_name

    # Bygg rad
    line = f"{emoji} {name_colored} {symbol_colored}"

    # Lägg till detail om det finns (vid done)
    if detail and state == "done":
        line = f"{line} ({detail})"

    # Lägg till felmeddelande om det finns
    if error_msg and state == "failed":
        error_colored = colorize(f"- {error_msg}", "red")
        line = f"{line} {error_colored}"

    # Thread-safe print
    with _print_lock:
        print(line, file=sys.stderr, flush=True)


def short_id(node_id: str) -> str:
    """Shorten UUID for terminal display: 'a2453abe-...' """
    if node_id and len(node_id) > 12 and '-' in node_id:
        return f"{node_id[:8]}-..."
    return node_id


def node_name(node: dict) -> str:
    """Get node name from properties dict."""
    return node.get("properties", {}).get("name", node.get("id", "?"))


def entity_detail(
    source_name: str,
    source_type: str,
    edge_type: str,
    target_name: str,
    target_type: str
) -> None:
    """
    Visa en sparad relation (indenterad under processing-raden).

    Args:
        source_name: Källnodens namn (t.ex. "Cenk")
        source_type: Källnodens typ (t.ex. "Person")
        edge_type: Relationstyp (t.ex. "WORKS_AT")
        target_name: Målnodens namn (t.ex. "Digitalist")
        target_type: Målnodens typ (t.ex. "Organization")

    Output:
        "   Cenk (Person) → WORKS_AT → Digitalist (Organization)"
    """
    arrow = "→"
    line = f"   {source_name} ({source_type}) {arrow} {edge_type} {arrow} {target_name} ({target_type})"

    with _print_lock:
        print(line, file=sys.stderr, flush=True)


def service_status(name: str, state: str, port: int = None) -> None:
    """
    Visa tjänst-status (för start_services.py).

    Args:
        name: Tjänstnamn
        state: started, stopped, failed
        port: Portnummer (valfritt)
    """
    emoji = "⚙️"

    if state == "started":
        symbol = colorize("●", "green")
        name_colored = colorize(name, "green")
    elif state == "stopped":
        symbol = colorize("○", "yellow")
        name_colored = colorize(name, "yellow")
    elif state == "failed":
        symbol = colorize("✗", "red")
        name_colored = colorize(name, "red")
    else:
        symbol = "◦"
        name_colored = name

    line = f"{emoji} {name_colored} {symbol}"
    if port:
        line = f"{line} (:{port})"

    with _print_lock:
        print(line, file=sys.stderr, flush=True)


def header(text: str) -> None:
    """Skriv en header-rad."""
    with _print_lock:
        print(f"\n{colorize(text, 'bold')}\n", file=sys.stderr, flush=True)


def separator() -> None:
    """Skriv en separator-linje."""
    with _print_lock:
        print("─" * 40, file=sys.stderr, flush=True)


def ready_message() -> None:
    """Visa 'Ready' meddelande."""
    with _print_lock:
        print(f"\n{colorize('--- Ready ---', 'green')}\n", file=sys.stderr, flush=True)


# Convenience class för att hålla state under processing
class TerminalStatus:
    """
    Context manager för att visa processing → done/failed.

    Användning:
        with TerminalStatus("ingestion", "file.pdf") as ts:
            # ... gör arbetet ...
            if error:
                ts.fail("Anledning")
        # Automatiskt "done" om inget fel
    """

    def __init__(self, process: str, item: str):
        self.process = process
        self.item = item
        self._failed = False
        self._error_msg = None

    def __enter__(self):
        status(self.process, self.item, "processing")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # Exception occurred
            self._failed = True
            self._error_msg = str(exc_val) if exc_val else "Unknown error"

        if self._failed:
            status(self.process, self.item, "failed", self._error_msg)
        else:
            status(self.process, self.item, "done")

        return False  # Don't suppress exceptions

    def fail(self, message: str) -> None:
        """Markera som failed med meddelande."""
        self._failed = True
        self._error_msg = message

    def skip(self, reason: str = None) -> None:
        """Markera som skipped."""
        status(self.process, self.item, "skipped")
        self._failed = True  # Prevent "done" in __exit__
