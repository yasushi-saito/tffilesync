import tensorflow as tf
import tensorflow.io.gfile as gfile
import tensorflow.compat.v1.logging as logging
import tffilesync
import time
import os
import unittest
import tempfile

logging.set_verbosity(logging.DEBUG)


def _kick_sync(syncer: tffilesync.Syncer):
    epoch = syncer.epoch()
    syncer.kick()
    while epoch == syncer.epoch():
        time.sleep(0.1)


def _read_file(path: str) -> str:
    with open(path, 'r') as fd:
        return fd.read()


class TestSync(unittest.TestCase):
    def test_basic(self):
        with tempfile.TemporaryDirectory() as remotedir, \
             tempfile.TemporaryDirectory() as localdir:
            with open(remotedir + '/f0.txt', 'w') as fd:
                fd.write('hello0')
            syncer = tffilesync.Syncer(remotedir, localdir)
            self.assertListEqual(gfile.listdir(localdir), ['f0.txt'])
            self.assertEqual(_read_file(localdir + '/f0.txt'), 'hello0')
            with open(localdir + '/f1.txt', 'w') as fd:
                fd.write('hello1')
            _kick_sync(syncer)

            self.assertListEqual(sorted(gfile.listdir(localdir)),
                                 ['f0.txt', 'f1.txt'])
            self.assertListEqual(sorted(gfile.listdir(remotedir)),
                                 ['f0.txt', 'f1.txt'])
            self.assertEqual(_read_file(remotedir + '/f0.txt'), 'hello0')
            self.assertEqual(_read_file(remotedir + '/f1.txt'), 'hello1')

            time.sleep(5)
            with open(localdir + '/f1.txt', 'w') as fd:
                fd.write('hello2')
            _kick_sync(syncer)
            self.assertEqual(_read_file(remotedir + '/f1.txt'), 'hello2')
            self.assertListEqual(sorted(gfile.listdir(remotedir)),
                                 ['f0.txt', 'f1.txt'])

            syncer.stop()


if __name__ == '__main__':
    unittest.main()
