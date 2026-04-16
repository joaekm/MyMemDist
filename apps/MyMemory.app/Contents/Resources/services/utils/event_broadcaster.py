"""Event-broadcaster för server → klient-events via SSE (#214).

Ingestion-workern skriver NOTIFY document_events vid status-ändringar. En
async task i service_api håller en persistent LISTEN-koppling till PG och
publicerar vidare till per-tenant asyncio-queues. Varje SSE-connection
subscribe:ar en queue filtrerad på tenant_id från JWT-claims.

Design:
- In-memory (single service_api-process). Multi-instance är ok out-of-the-box
  eftersom PG LISTEN/NOTIFY broadcastar till alla lyssnare — varje instans
  ser alla events och filtrerar till sina anslutna klienter.
- Ingen persistens — vid service_api-restart tappas anslutna klienter, och
  events som publicerades under omstart missas. Klienten reconnectar
  automatiskt; eventuella missade done-övergångar är inget problem eftersom
  filen redan är persisterad i documents-tabellen — klienten kan inspektera
  state vid behov, men för notisflödet räcker det med att nya events kommer
  igenom.
"""

from __future__ import annotations

import asyncio
import json
import logging
import select
from typing import AsyncIterator, Optional

import psycopg2
import psycopg2.extensions

from services.utils.graph_service import _get_pg_dsn

LOGGER = logging.getLogger('EVENT_BROADCASTER')

# PG-kanal som ingestion-workern NOTIFY:ar på
_PG_CHANNEL = "document_events"

# Max events i kö per subscriber innan de börjar droppas. En snabb klient
# som lyssnar normalt har noll-kö — denna gräns hindrar bara minnesläckage
# om en klient slutar konsumera men inte kopplar ner.
_QUEUE_MAXSIZE = 1024


class EventBroadcaster:
    """Per-tenant fan-out av document_events."""

    def __init__(self) -> None:
        self._subscribers: dict[str, list[asyncio.Queue]] = {}
        self._lock = asyncio.Lock()

    async def subscribe(self, tenant_id: str) -> asyncio.Queue:
        """Skapa ny subscriber-queue för en tenant. Caller ansvarar för
        att unsubscribe när klienten kopplar ner."""
        queue: asyncio.Queue = asyncio.Queue(maxsize=_QUEUE_MAXSIZE)
        async with self._lock:
            self._subscribers.setdefault(tenant_id, []).append(queue)
        return queue

    async def unsubscribe(self, tenant_id: str, queue: asyncio.Queue) -> None:
        async with self._lock:
            if tenant_id in self._subscribers:
                self._subscribers[tenant_id] = [
                    q for q in self._subscribers[tenant_id] if q is not queue
                ]
                if not self._subscribers[tenant_id]:
                    del self._subscribers[tenant_id]

    async def publish(self, tenant_id: str, event: dict) -> None:
        """Pusha event till alla subscribers för tenant_id. Full queue →
        WARNING + drop (klienten hänger förmodligen — bättre att tappa
        events än att blockera hela broadcastern)."""
        async with self._lock:
            queues = list(self._subscribers.get(tenant_id, []))
        for q in queues:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                LOGGER.warning(
                    f"Subscriber queue full för tenant {tenant_id[:8]}..., "
                    f"droppar event för uuid={event.get('uuid')}"
                )


# --- PG LISTEN-loop ---

def _wait_for_notify(conn: psycopg2.extensions.connection, timeout: float) -> bool:
    """Blockerande select() tills notify dyker upp eller timeout. Körs i
    tråd via asyncio.to_thread så vi inte blockerar event-loopen."""
    r, _, _ = select.select([conn], [], [], timeout)
    return bool(r)


async def run_listen_loop(broadcaster: EventBroadcaster) -> None:
    """Persistent PG LISTEN → publicera till broadcaster. Reconnectar
    automatiskt vid fel. Körs som async task i service_api."""
    backoff = 1.0
    while True:
        conn: Optional[psycopg2.extensions.connection] = None
        try:
            conn = psycopg2.connect(_get_pg_dsn())
            conn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
            )
            with conn.cursor() as cur:
                cur.execute(f"LISTEN {_PG_CHANNEL}")
            LOGGER.info(f"Lyssnar på PG-kanal '{_PG_CHANNEL}'")
            backoff = 1.0  # reset efter lyckad koppling

            while True:
                got_event = await asyncio.to_thread(_wait_for_notify, conn, 30.0)
                if not got_event:
                    # Keepalive: ping PG så att TCP-connection hålls levande
                    # genom proxys/NAT med kort idle-timeout
                    conn.poll()
                    continue
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    try:
                        payload = json.loads(notify.payload)
                        tenant_id = payload.get('tenant_id')
                        if not tenant_id:
                            LOGGER.warning(
                                f"NOTIFY utan tenant_id: {notify.payload[:200]}"
                            )
                            continue
                        await broadcaster.publish(tenant_id, payload)
                    except json.JSONDecodeError as e:
                        LOGGER.error(
                            f"Kunde inte parsa NOTIFY payload: {e}, "
                            f"raw={notify.payload[:200]}"
                        )
        except psycopg2.Error as e:
            LOGGER.error(
                f"LISTEN-loop fel: {type(e).__name__}: {e}. "
                f"Reconnectar om {backoff:.0f}s."
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except psycopg2.Error:
                    pass


# --- SSE event-format ---

def format_sse_event(event_name: str, data: dict) -> bytes:
    """Formattera ett event enligt SSE-spec:
    event: <name>
    data: <json>
    \n
    """
    payload = json.dumps(data, ensure_ascii=False)
    return f"event: {event_name}\ndata: {payload}\n\n".encode('utf-8')


def format_sse_keepalive() -> bytes:
    """SSE-kommentar som keepalive — klient ignorerar, men TCP hålls vaken."""
    return b": keep-alive\n\n"


async def sse_event_stream(
    broadcaster: EventBroadcaster, tenant_id: str
) -> AsyncIterator[bytes]:
    """Async iterator som ger SSE-rader tills klienten kopplar ner.

    Heartbeat (\": keep-alive\") var 15s så Caddy/proxys inte dödar idle
    connections.
    """
    queue = await broadcaster.subscribe(tenant_id)
    try:
        # Initial ready-event — klienten vet att anslutningen är live
        yield format_sse_event("ready", {"tenant_id": tenant_id})

        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=15.0)
            except asyncio.TimeoutError:
                yield format_sse_keepalive()
                continue
            # Mappa PG-event-fält till SSE-event-namn
            status = event.get('status', 'unknown')
            event_name = {
                'done': 'ingestion_done',
                'failed': 'ingestion_failed',
            }.get(status, 'ingestion_update')
            yield format_sse_event(event_name, event)
    finally:
        await broadcaster.unsubscribe(tenant_id, queue)
