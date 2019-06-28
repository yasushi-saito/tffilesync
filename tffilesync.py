import tensorflow as tf
import tensorflow.io.gfile as gfile
import tensorflow.compat.v1.logging as logging
import time
import threading
from typing import NamedTuple, Dict, Iterable

_FileStat = NamedTuple('_FileStats', [('length', int), ('mtime_nsec', int),
                                      ('is_directory', bool)])
_DirEntries = Dict[str, _FileStat]


def _list_dir(dir_path: str) -> _DirEntries:
    ents: _DirEntries = {}
    for name in gfile.listdir(dir_path):
        path = dir_path + '/' + name
        stat = gfile.stat(path)
        ents[name] = _FileStat(length=stat.length,
                               mtime_nsec=stat.mtime_nsec,
                               is_directory=stat.is_directory)
    return ents


def _copy_file(src_dir: str, dest_dir: str, file_name: str):
    src_path = src_dir + '/' + file_name
    dest_path = dest_dir + '/' + file_name
    for retries in range(0, 10):
        try:
            gfile.copy(src_path, dest_path, overwrite=True)
            logging.info("copy %s->%s succeeded (retry %d)", src_path,
                         dest_path, retries)
            return
        except tf.errors.OpError as ex:
            logging.error("copy %s->%s (retry %d): %s", src_path, dest_path,
                          retries, ex)
            time.sleep(1.5**retries)


def _has_file(dir_ents: _DirEntries, name: str, want_stat: _FileStat):
    if name not in dir_ents:
        return False
    got_stat = dir_ents[name]
    if got_stat.length != want_stat.length:
        return False
    got_mtime_s = got_stat.mtime_nsec // 1000000000
    want_mtime_s = want_stat.mtime_nsec // 1000000000

    if abs(got_mtime_s - want_mtime_s) > 3:
        return False
    return True


_FULL_SYNC_INTERVAL_S = 6 * 60


class Syncer:
    def __init__(self, remote_dir: str, local_dir: str):
        self._remote_dir = remote_dir
        self._local_dir = local_dir
        self._mu = threading.Lock()
        self._cond = threading.Condition(lock=self._mu)
        self._stopping = False
        self._epoch = 0
        gfile.makedirs(local_dir)

        remote_ents = _list_dir(remote_dir)
        local_ents = _list_dir(local_dir)

        for name, ent in remote_ents.items():
            if not _has_file(local_ents, name, ent):
                _copy_file(remote_dir, local_dir, name)

        self._thread = threading.Thread(target=self._loop)
        self._thread.start()

    def epoch(self) -> int:
        with self._mu:
            return self._epoch

    def stop(self) -> None:
        with self._mu:
            self._stopping = True
            self._cond.notify()
        self._thread.join()

    def kick(self) -> None:
        with self._mu:
            self._cond.notify()

    def _loop(self) -> None:
        src_ents: _DirEntries = {}
        last_full_sync_time = time.time()
        while True:
            with self._mu:
                self._epoch += 1
                self._cond.wait(60.0)
                if self._stopping:
                    return

            now = time.time()
            if now - last_full_sync_time >= _FULL_SYNC_INTERVAL_S:
                src_ents = {}

            new_ents = _list_dir(self._local_dir)
            for name, ent in new_ents.items():
                if not _has_file(src_ents, name, ent):
                    # file added or updated
                    _copy_file(self._local_dir, self._remote_dir, name)
                    src_ents[name] = ent
