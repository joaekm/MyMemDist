"""
Shared Lock - Process-safe resource locking for MyMemory.

Provides file-based locking using fcntl for coordination between
separate processes (e.g., ingestion_engine and dreamer_daemon).

Supports Reader-Writer pattern:
- Multiple readers can hold shared locks simultaneously
- Writers require exclusive lock (blocks all readers and other writers)

Usage:
    from services.utils.shared_lock import resource_lock

    # Exclusive lock for writing
    with resource_lock("graph", exclusive=True):
        graph.upsert_node(...)

    # Shared lock for reading
    with resource_lock("graph", exclusive=False):
        results = graph.search(...)
"""

import fcntl
import logging
import os
import time
from contextlib import contextmanager
from typing import Optional

import yaml

LOGGER = logging.getLogger("SharedLock")

# Cache for lock directory path
_lock_dir: Optional[str] = None


def _get_lock_dir() -> str:
    """Get lock directory from config. HARDFAIL if config not found."""
    global _lock_dir
    if _lock_dir is not None:
        return _lock_dir

    # Find config relative to this file
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'my_mem_config.yaml')

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"HARDFAIL: Config not found at {config_path}")

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # Get lock_dir from config, or derive from index path
    lock_dir = config.get('paths', {}).get('lock_dir')
    if lock_dir:
        _lock_dir = os.path.expanduser(lock_dir)
    else:
        # Derive from index path (same directory as graph_db)
        graph_path = config.get('paths', {}).get('graph_db')
        if graph_path:
            index_dir = os.path.dirname(os.path.expanduser(graph_path))
            _lock_dir = os.path.join(index_dir, '.locks')
        else:
            raise ValueError("HARDFAIL: Neither 'lock_dir' nor 'graph_db' found in config paths")

    return _lock_dir


@contextmanager
def resource_lock(resource: str, exclusive: bool = True, timeout: Optional[float] = None):
    """
    Process-safe lock for shared resources.

    Uses fcntl.flock for cross-process coordination. Supports both
    exclusive (write) and shared (read) locks.

    Args:
        resource: Resource name ("graph", "vector", "lake", "dreamer")
        exclusive: True for write lock (LOCK_EX), False for read lock (LOCK_SH)
        timeout: Optional timeout in seconds. None = block forever.
                 If timeout expires, raises TimeoutError.

    Yields:
        None (context manager)

    Raises:
        TimeoutError: If timeout specified and lock not acquired in time
        OSError: If locking fails for other reasons

    Example:
        # Exclusive lock for graph writes
        with resource_lock("graph", exclusive=True):
            graph.upsert_node(...)

        # Shared lock for concurrent reads
        with resource_lock("graph", exclusive=False):
            results = graph.search(...)

        # With timeout
        try:
            with resource_lock("graph", exclusive=True, timeout=5.0):
                do_work()
        except TimeoutError:
            LOGGER.warning("Could not acquire lock in time")
    """
    lock_dir = _get_lock_dir()
    os.makedirs(lock_dir, exist_ok=True)

    lock_file_path = os.path.join(lock_dir, f"{resource}.lock")
    lock_type = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
    lock_type_str = "EXCLUSIVE" if exclusive else "SHARED"

    # Open lock file (create if not exists)
    lock_file = open(lock_file_path, "w")

    try:
        if timeout is not None:
            # Non-blocking with retry loop
            start_time = time.monotonic()
            while True:
                try:
                    fcntl.flock(lock_file, lock_type | fcntl.LOCK_NB)
                    LOGGER.debug(f"Acquired {lock_type_str} lock on {resource}")
                    break
                except BlockingIOError:
                    # Expected when lock is held - retry until timeout
                    elapsed = time.monotonic() - start_time
                    if elapsed >= timeout:
                        lock_file.close()
                        raise TimeoutError(
                            f"Could not acquire {lock_type_str} lock on {resource} "
                            f"within {timeout}s"
                        )
                    time.sleep(0.05)
        else:
            # Blocking acquire
            LOGGER.debug(f"Waiting for {lock_type_str} lock on {resource}...")
            fcntl.flock(lock_file, lock_type)
            LOGGER.debug(f"Acquired {lock_type_str} lock on {resource}")

        yield

    finally:
        # Release lock - best effort, log but don't raise
        try:
            fcntl.flock(lock_file, fcntl.LOCK_UN)
            LOGGER.debug(f"Released {lock_type_str} lock on {resource}")
        except OSError as e:
            # Lock release failure is logged but not fatal
            LOGGER.warning(f"Could not release lock on {resource}: {e}")
        lock_file.close()


def is_locked(resource: str) -> bool:
    """
    Check if a resource is currently locked (non-blocking).

    Args:
        resource: Resource name to check

    Returns:
        True if resource is locked by another process, False if available
    """
    lock_dir = _get_lock_dir()
    lock_file_path = os.path.join(lock_dir, f"{resource}.lock")

    if not os.path.exists(lock_file_path):
        return False

    try:
        with open(lock_file_path, "w") as f:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(f, fcntl.LOCK_UN)
            return False
    except BlockingIOError:
        LOGGER.debug(f"Resource {resource} is currently locked")
        return True


def clear_stale_locks():
    """
    Clear any stale lock files.

    Should only be called during system startup or after crash recovery.
    WARNING: Do not call while processes might be holding locks!
    """
    lock_dir = _get_lock_dir()
    if not os.path.exists(lock_dir):
        return

    for filename in os.listdir(lock_dir):
        if filename.endswith(".lock"):
            lock_path = os.path.join(lock_dir, filename)
            try:
                os.remove(lock_path)
                LOGGER.info(f"Cleared stale lock: {filename}")
            except OSError as e:
                LOGGER.warning(f"Could not remove lock file {filename}: {e}")
