"""Bytes puts copy the buffer directly. Spawn is rejected."""

import multiprocessing
import subprocess
import sys
from queue import Empty, Full
from unittest import TestCase

from faster_fifo import Queue, QueueError


def _identity_dumps(obj):
    return obj


def _identity_loads(raw):
    return bytes(raw)


def _child_put_bytes(queue, control):
    queue.put(b"from-child")
    control.put(b"alive")


def _child_put_many(queue, control):
    queue.put_many([b"one", b"two"])
    control.put(b"alive")


def _touch(queue):
    queue.put(1)


class TestForkBytesPut(TestCase):
    def _queue(self, max_size_bytes=1000 * 1000):
        return Queue(
            max_size_bytes=max_size_bytes,
            dumps=_identity_dumps,
            loads=_identity_loads,
        )

    def test_bytes_skip_dumps(self):
        seen = []

        def dumps(obj):
            seen.append(obj)
            return obj if type(obj) is bytes else b"not-bytes"

        q = Queue(dumps=dumps, loads=_identity_loads)
        q.put(b"abc")
        q.put_many((b"d", b"e"))
        q.put({"n": 1})
        self.assertEqual(q.get(), b"abc")
        self.assertEqual(q.get_many(max_messages_to_get=2), [b"d", b"e"])
        self.assertEqual(seen, [{"n": 1}])
        self.assertEqual(q.get(), b"not-bytes")

    def test_fork_child_put_bytes(self):
        ctx = multiprocessing.get_context("fork")
        queue = self._queue()
        control = self._queue()
        proc = ctx.Process(target=_child_put_bytes, args=(queue, control))
        proc.start()
        self.assertEqual(control.get(timeout=5), b"alive")
        self.assertEqual(queue.get(timeout=5), b"from-child")
        proc.join(5)
        self.assertEqual(proc.exitcode, 0)

    def test_fork_child_put_many_bytes(self):
        ctx = multiprocessing.get_context("fork")
        queue = self._queue()
        control = self._queue()
        proc = ctx.Process(target=_child_put_many, args=(queue, control))
        proc.start()
        self.assertEqual(control.get(timeout=5), b"alive")
        self.assertEqual(queue.get_many(max_messages_to_get=2), [b"one", b"two"])
        proc.join(5)
        self.assertEqual(proc.exitcode, 0)

    def test_fast_path_full_raises(self):
        q = self._queue(max_size_bytes=60)
        q.put(b"x" * 40, block=False)
        with self.assertRaises(Full):
            q.put_many([b"y" * 40, b"z" * 40], block=False)
        self.assertEqual(q.qsize(), 1)
        self.assertEqual(q.get(), b"x" * 40)

    def test_spawn_child_is_rejected(self):
        ctx = multiprocessing.get_context("spawn")
        queue = Queue()
        proc = ctx.Process(target=_touch, args=(queue,))
        proc.start()
        proc.join(10)
        self.assertNotEqual(proc.exitcode, 0)

    def test_spawn_start_method_is_rejected(self):
        code = "\n".join(
            [
                "import multiprocessing",
                "multiprocessing.set_start_method('spawn')",
                "from faster_fifo import Queue, QueueError",
                "try:",
                "    Queue()",
                "except QueueError as exc:",
                "    if 'fork' not in str(exc):",
                "        raise SystemExit(2)",
                "else:",
                "    raise SystemExit(3)",
            ]
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


class TestFastGet(TestCase):
    def _queue(self):
        return Queue(dumps=_identity_dumps, loads=_identity_loads)

    def test_get_many_preserves_order(self):
        queue = self._queue()
        queue.put_many([b"a", b"bb", b"ccc"])
        self.assertEqual(queue.get_many(max_messages_to_get=3), [b"a", b"bb", b"ccc"])
        self.assertTrue(queue.empty())

    def test_get_nowait_empty_raises(self):
        queue = self._queue()
        with self.assertRaises(Empty):
            queue.get_nowait()

    def test_get_grows_receive_buffer(self):
        queue = self._queue()
        payload = b"z" * 8000
        queue.put(payload)
        self.assertEqual(queue.get(), payload)

    def test_loads_sees_each_message_once(self):
        seen = []

        def loads(raw):
            seen.append(bytes(raw))
            return bytes(raw)

        queue = Queue(dumps=_identity_dumps, loads=loads)
        queue.put(b"one")
        queue.put_many([b"two", b"three"])
        self.assertEqual(queue.get(), b"one")
        self.assertEqual(queue.get_many(max_messages_to_get=2), [b"two", b"three"])
        self.assertEqual(seen, [b"one", b"two", b"three"])

    def test_pickled_object_roundtrip(self):
        queue = Queue()
        queue.put({"k": [1, 2, 3]})
        self.assertEqual(queue.get(), {"k": [1, 2, 3]})


class TestQueueErrorImport(TestCase):
    def test_queue_error_is_public(self):
        self.assertTrue(issubclass(QueueError, Exception))
