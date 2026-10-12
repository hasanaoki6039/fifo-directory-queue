import os
import shutil
import tempfile
import unittest

from fifo_directory_queue import DirectoryQueue, EmptyQueue


class _FakeClock:
    def __init__(self, start=1000.0):
        self.t = start

    def __call__(self):
        return self.t

    def advance(self, n=1.0):
        self.t += n


class TestEnqueueDequeue(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.clock = _FakeClock()
        self.q = DirectoryQueue(self.tmp, clock=self.clock)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_enqueue_returns_ready_name(self):
        name = self.q.enqueue(b"hello")
        self.assertTrue(name.endswith(".ready"))
        self.assertIn(name, os.listdir(os.path.join(self.tmp, "ready")))

    def test_dequeue_returns_payload_and_name(self):
        self.q.enqueue(b"hello")
        item = self.q.dequeue()
        self.assertEqual(item["data"], b"hello")
        self.assertTrue(item["name"].endswith(".ready"))
        self.assertTrue(item["lock"].endswith(".lock"))

    def test_fifo_order(self):
        self.q.enqueue(b"one")
        self.q.enqueue(b"two")
        self.q.enqueue(b"three")
        self.assertEqual(self.q.dequeue()["data"], b"one")
        self.assertEqual(self.q.dequeue()["data"], b"two")
        self.assertEqual(self.q.dequeue()["data"], b"three")

    def test_dequeue_empty_raises(self):
        with self.assertRaises(EmptyQueue):
            self.q.dequeue()

    def test_acknowledge_deletes_lock(self):
        self.q.enqueue(b"x")
        item = self.q.dequeue()
        self.q.acknowledge(item["lock"])
        self.assertEqual(os.listdir(os.path.join(self.tmp, "lock")), [])

    def test_unacknowledged_items_are_in_lock_names(self):
        self.q.enqueue(b"a")
        self.q.enqueue(b"b")
        first = self.q.dequeue()
        second = self.q.dequeue()
        locks = self.q.lock_names()
        self.assertEqual(len(locks), 2)
        self.assertEqual(locks[0], first["lock"])
        self.assertEqual(locks[1], second["lock"])

    def test_dequeue_after_acknowledge_gets_next(self):
        self.q.enqueue(b"first")
        self.q.enqueue(b"second")
        item = self.q.dequeue()
        self.q.acknowledge(item["lock"])
        self.assertEqual(self.q.dequeue()["data"], b"second")


class TestStagingAndReap(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.q = DirectoryQueue(self.tmp, clock=_FakeClock())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_reap_removes_stale_stage_files(self):
        stage_dir = os.path.join(self.tmp, "stage")
        with open(os.path.join(stage_dir, "junk.stage"), "wb") as f:
            f.write(b"x")
        n = self.q.reap_stale_staging()
        self.assertEqual(n, 1)
        self.assertEqual(os.listdir(stage_dir), [])

    def test_reap_does_not_touch_ready(self):
        self.q.enqueue(b"alive")
        n = self.q.reap_stale_staging()
        self.assertEqual(n, 0)
        self.assertEqual(len(os.listdir(os.path.join(self.tmp, "ready"))), 1)


class TestCounterPersistence(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.clock = _FakeClock()
        self.q = DirectoryQueue(self.tmp, clock=self.clock)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_new_instance_skips_used_counters(self):
        self.q.enqueue(b"a")
        self.q.enqueue(b"b")
        q2 = DirectoryQueue(self.tmp, clock=self.clock)
        q2.enqueue(b"c")
        ready = sorted(os.listdir(os.path.join(self.tmp, "ready")))
        self.assertEqual(len(ready), 3)
        counters = [int(n.split(".")[0]) for n in ready]
        self.assertEqual(counters, sorted(counters))
        self.assertEqual(len(set(counters)), 3)

    def test_state_file_missing_recovers_from_dirs(self):
        self.q.enqueue(b"a")
        self.q.enqueue(b"b")
        os.remove(os.path.join(self.tmp, "state"))
        q2 = DirectoryQueue(self.tmp, clock=self.clock)
        q2.enqueue(b"c")
        ready = sorted(os.listdir(os.path.join(self.tmp, "ready")))
        counters = [int(n.split(".")[0]) for n in ready]
        self.assertEqual(len(set(counters)), 3)

    def test_state_file_corrupt_recovers_from_dirs(self):
        self.q.enqueue(b"a")
        with open(os.path.join(self.tmp, "state"), "w") as f:
            f.write("not json")
        q2 = DirectoryQueue(self.tmp, clock=self.clock)
        q2.enqueue(b"b")
        ready = sorted(os.listdir(os.path.join(self.tmp, "ready")))
        counters = [int(n.split(".")[0]) for n in ready]
        self.assertEqual(len(set(counters)), 2)


class TestInputValidation(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.q = DirectoryQueue(self.tmp, clock=_FakeClock())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_enqueue_rejects_str(self):
        with self.assertRaises(TypeError):
            self.q.enqueue("not bytes")

    def test_acknowledge_rejects_bad_name(self):
        with self.assertRaises(ValueError):
            self.q.acknowledge("00000000000000000000.deadbeef.lock")
        with self.assertRaises(ValueError):
            self.q.acknowledge("not-a-lock")


class TestLockFileIsolation(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.clock = _FakeClock()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_dequeue_moves_to_lock_dir(self):
        q = DirectoryQueue(self.tmp, clock=self.clock)
        q.enqueue(b"x")
        item = q.dequeue()
        ready = os.listdir(os.path.join(self.tmp, "ready"))
        locks = os.listdir(os.path.join(self.tmp, "lock"))
        self.assertEqual(ready, [])
        self.assertEqual(locks, [item["lock"]])


if __name__ == "__main__":
    unittest.main()
