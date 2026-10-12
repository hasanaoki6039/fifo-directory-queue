"""fifo-directory-queue: a crash-safe FIFO work queue backed by a directory."""

from .core import DirectoryQueue, DequeueError, EmptyQueue

__all__ = ["DirectoryQueue", "DequeueError", "EmptyQueue"]
