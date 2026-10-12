# fifo-directory-queue

A crash-safe FIFO work queue that stores items as files in a directory. No database, no external services, no third-party dependencies.

```python
from fifo_directory_queue import DirectoryQueue

q = DirectoryQueue("./queue")

q.enqueue(b"job payload")            # returns a ready filename
item = q.dequeue()                   # raises EmptyQueue if none ready
# item == {"data": bytes, "name": str, "lock": str}
print(item["data"])
q.acknowledge(item["lock"])           # delete the lock to complete

# Recover unfinished items after a crash:
for lock_name in q.lock_names():
    print(lock_name)
q.reap_stale_staging()              # delete orphaned staging files
```

## Why this exists

You need a durable work queue — items survive a process restart — but a database is too heavy and a broker is too much infrastructure. A directory is already a perfectly good queue if you are careful about atomicity.

The trade-off: throughput is bounded by filesystem operations (one `rename` and one `fsync` per enqueue), and ordering is based on an insertion counter rather than wall-clock time, so two items enqueued in the same second still have a well-defined order.

## Why filenames instead of mtime

`os.stat().st_mtime` has one-second resolution on many filesystems, so files enqueued within the same second tie. To get a strict total order this library encodes a monotonic counter in each filename (zero-padded to 20 digits so lexicographic sort matches numeric sort). The counter is persisted in a `state` file before use; if the state file is lost or corrupt, the library scans the `ready/` and `lock/` directories to recover the highest counter in use.

## The awkward edge

Dequeue is a two-step operation: `dequeue()` renames the oldest ready file into a `lock/` file and returns its contents; the consumer must call `acknowledge()` to delete the lock once processing is done. If the process crashes between those steps the lock file stays on disk. Use `lock_names()` to discover such unfinished items and decide whether to reprocess them or discard them — the library does not guess for you.
