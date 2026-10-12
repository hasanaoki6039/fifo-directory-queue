"""
FIFO Directory Queue — a crash-safe work queue that uses a directory on disk
instead of a database.

Files are atomically enqueued by renaming from a staging directory into a ready
directory. Dequeue scans the ready directory, sorts entries by insertion counter
(encoded in the filename), and renames the oldest into a lock file before
returning its path to the caller. The caller processes the file and then calls
acknowledge() to delete the lock, completing the dequeue.

Why filenames instead of mtime:
  os.stat().st_mtime has only one-second resolution on many filesystems, so
  two files enqueued within the same second tie. Using a monotonic counter in
  the filename gives a strict total order that is also stable across clock
  skew and across NFS (where mtime is server-dependent and often coarse).

Atomicity contracts:
  - Enqueue: create temp file in staging dir, write payload, fsync, rename to
    ready dir. The rename is atomic on POSIX and Windows. A crash before the
    rename leaves a harmless staging file, never a half-written ready file.
  - Dequeue: rename ready → lock. A rename is atomic, so only one process can
    hold a given lock file. A crash after the rename leaves the lock file in
    place; it is recoverable by scan_locks().
"""

from __future__ import annotations

import json
import os
import re
import secrets
import time
from typing import Callable, Dict, List, Optional


class DequeueError(Exception):
    """Base class for dequeue-time errors."""


class EmptyQueue(DequeueError):
    """Raised by dequeue() when no ready items are available."""


# Filename layout in the ready directory:
#     <counter>.<nonce>.ready
# counter  — zero-padded to 20 digits so lexicographic sort matches numeric sort.
# nonce    — random hex to prevent filename collisions if two processes grab the
#            same counter (extremely unlikely with a state file, but cheap to
#            guard against, and it protects against accidental counter reuse
#            after a crash + manual state file reset).
_READY_RE = re.compile(r"^(\d{20})\.([0-9a-f]+)\.ready$")
_LOCK_RE = re.compile(r"^(\d{20})\.([0-9a-f]+)\.lock$")


class DirectoryQueue:
    """A crash-safe FIFO queue that persists items as files in a directory.

    Layout under ``base_dir``::

        state   — JSON file holding the next monotonic counter.
        stage/  — transient directory for in-progress enqueues.
        ready/  — committed items waiting to be dequeued.
        lock/   — items that have been dequeued but not yet acknowledged.

    Parameters
    ----------
    base_dir:
        Root directory for the queue. Created if it does not exist.
    clock:
        Callable returning the current time as a float, in seconds. Used only
        for formatting the state-file mtime; does not affect ordering. Inject a
        fake in tests to keep them deterministic.
    """

    def __init__(
        self,
        base_dir: str,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._base_dir = os.path.abspath(base_dir)
        self._stage_dir = os.path.join(self._base_dir, "stage")
        self._ready_dir = os.path.join(self._base_dir, "ready")
        self._lock_dir = os.path.join(self._base_dir, "lock")
        self._state_file = os.path.join(self._base_dir, "state")
        self._clock = clock

        os.makedirs(self._stage_dir, exist_ok=True)
        os.makedirs(self._ready_dir, exist_ok=True)
        os.makedirs(self._lock_dir, exist_ok=True)

        self._next_counter = self._load_counter()

    # ------------------------------------------------------------------ #
    # Enqueue                                                            #
    # ------------------------------------------------------------------ #

    def enqueue(self, data: bytes) -> str:
        """Atomically enqueue ``data`` and return the ready-filename.

        ``data`` must be ``bytes``; the caller is responsible for any
        encoding. The staging write is fsynced so that a crash never produces
        a truncated ready file (the rename into ready/ is the commit point).
        """
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("data must be bytes")

        counter = self._next_counter
        self._next_counter += 1
        self._write_state(self._next_counter)

        nonce = secrets.token_hex(8)
        ready_name = f"{counter:020d}.{nonce}.ready"

        # Write to a staging file first so the ready directory never sees a
        # partially written file. A crash here leaves junk in stage/, never a
        # corrupt ready item.
        staging_name = f"{counter:020d}.{nonce}.stage"
        staging_path = os.path.join(self._stage_dir, staging_name)
        ready_path = os.path.join(self._ready_dir, ready_name)

        with open(staging_path, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())

        os.replace(staging_path, ready_path)  # atomic on POSIX and Windows
        return ready_name

    # ------------------------------------------------------------------ #
    # Dequeue                                                            #
    # ------------------------------------------------------------------ #

    def _scan_ready(self) -> List[str]:
        """Return ready filenames sorted by insertion order (oldest first)."""
        entries: List[tuple] = []
        for name in os.listdir(self._ready_dir):
            m = _READY_RE.match(name)
            if not m:
                continue
            counter = int(m.group(1))
            entries.append((counter, m.group(2), name))
        entries.sort(key=lambda e: (e[0], e[1]))
        return [e[2] for e in entries]

    def _scan_locks(self) -> List[str]:
        """Return lock filenames sorted by insertion order (oldest first)."""
        entries: List[tuple] = []
        for name in os.listdir(self._lock_dir):
            m = _LOCK_RE.match(name)
            if not m:
                continue
            counter = int(m.group(1))
            entries.append((counter, m.group(2), name))
        entries.sort(key=lambda e: (e[0], e[1]))
        return [e[2] for e in entries]

    def dequeue(self) -> Dict[str, object]:
        """Remove and return the oldest ready item.

        Returns a dict with keys:
          - ``data``   : the item payload as ``bytes``
          - ``name``   : the original ready filename
          - ``lock``   : the lock filename (pass to ``acknowledge``)

        Raises ``EmptyQueue`` if no ready items are available.

        The item is renamed from ready/ to lock/ atomically. The caller must
        call ``acknowledge()`` after processing to delete the lock and make
        the dequeue permanent. If the process crashes first, the lock file
        remains on disk and can be discovered with ``lock_names()``.
        """
        ready_files = self._scan_ready()
        if not ready_files:
            raise EmptyQueue("no ready items")

        for ready_name in ready_files:
            ready_path = os.path.join(self._ready_dir, ready_name)
            m = _READY_RE.match(ready_name)
            if m is None:
                continue
            counter = int(m.group(1))
            nonce = m.group(2)
            lock_name = f"{counter:020d}.{nonce}.lock"
            lock_path = os.path.join(self._lock_dir, lock_name)

            try:
                # rename = atomic claim. Only the process whose rename
                # succeeds holds this item.
                os.replace(ready_path, lock_path)
            except FileNotFoundError:
                # Another consumer dequeued it between our scan and our
                # rename; try the next-oldest.
                continue

            with open(lock_path, "rb") as f:
                data = f.read()

            return {
                "data": data,
                "name": ready_name,
                "lock": lock_name,
            }

        # All candidates were taken between scan and rename.
        raise EmptyQueue("no ready items")

    def acknowledge(self, lock_name: str) -> None:
        """Delete the lock file for a completed item.

        Called after the consumer has finished processing the item returned by
        ``dequeue()``. Until acknowledge is called the item remains in lock/
        and will be returned by ``lock_names()`` as an unfinished item.
        """
        if not _LOCK_RE.match(lock_name):
            raise ValueError(f"not a lock filename: {lock_name!r}")
        lock_path = os.path.join(self._lock_dir, lock_name)
        try:
            os.remove(lock_path)
        except FileNotFoundError:
            raise ValueError(f"no such lock: {lock_name!r}")

    def lock_names(self) -> List[str]:
        """Return lock filenames (dequeued-but-unacknowledged) oldest first."""
        return self._scan_locks()

    # ------------------------------------------------------------------ #
    # Maintenance                                                        #
    # ------------------------------------------------------------------ #

    def reap_stale_staging(self) -> int:
        """Delete orphaned staging files left by crashed enqueues.

        Enqueue writes a staging file and then renames it into ready. If the
        process crashes between those steps the staging file is orphaned.
        Such a file was never committed, so it is safe to delete. Returns the
        number of files removed.
        """
        removed = 0
        for name in os.listdir(self._stage_dir):
            if name.endswith(".stage"):
                try:
                    os.remove(os.path.join(self._stage_dir, name))
                    removed += 1
                except FileNotFoundError:
                    pass
        return removed

    # ------------------------------------------------------------------ #
    # State file                                                         #
    # ------------------------------------------------------------------ #

    def _load_counter(self) -> int:
        """Return the next monotonic counter to use.

        The counter is persisted *before* the enqueue completes, so a crash
        at any point after this call cannot cause the same counter to be
        handed out twice. If the state file is missing or unreadable we scan
        ready+lock for the highest existing counter and start above it; this
        is correct because counters are monotonic and zero-padded.
        """
        try:
            with open(self._state_file, "r", encoding="utf-8") as f:
                state = json.load(f)
            return int(state["next_counter"])
        except (FileNotFoundError, ValueError, KeyError, json.JSONDecodeError):
            return self._recover_counter()

    def _write_state(self, counter: int) -> None:
        payload = json.dumps({
            "next_counter": counter,
            "updated_at": self._clock(),
        })
        tmp = self._state_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._state_file)

    def _recover_counter(self) -> int:
        """Scan ready and lock dirs to find the highest counter in use."""
        highest = -1
        for directory in (self._ready_dir, self._lock_dir):
            try:
                names = os.listdir(directory)
            except FileNotFoundError:
                continue
            for name in names:
                m = _READY_RE.match(name) or _LOCK_RE.match(name)
                if m:
                    c = int(m.group(1))
                    if c > highest:
                        highest = c
        return highest + 1
