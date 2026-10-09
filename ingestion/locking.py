"""Nonblocking OS locks: crashes release locks without stale-file recovery."""
import fcntl
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def directory_lock(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"Another DevPulse process holds {path}; wait for it to finish") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
