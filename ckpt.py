import os
import socket
from itertools import chain

try:
    from rfbp_data import DistributedData, DistributedDataServer
    from sliding_list import SlidingList
except ImportError:
    from rfbp.rfbp_data import DistributedData, DistributedDataServer
    from rfbp.sliding_list import SlidingList


class CKPT(DistributedData):
    def __init__(self, ckpt=None, save_every=1, save_fn=None, restart=False,
                 print_info=False, update_interval=60, server_addr=None, lock=None):
        super(CKPT, self).__init__(filepath=ckpt, save_fn=save_fn, restart=restart,
                                   print_info=print_info, server_addr=server_addr, data_type=SlidingList,
                                   update_interval=update_interval, lock=lock)
        self.save_every = save_every

    def __getitem__(self, idx):
        self._lock()
        ret = self.data[idx]
        self._unlock()
        return bool(ret)

    def __setitem__(self, idx, value):
        self._lock()
        if (not self.data[idx]) and value:
            if self.print_info:
                print(os.getpid(), ": finish: ", idx)
            self.data[idx] = bool(value)
            self.save_count += 1
            if 0 < self.save_every <= self.save_count:
                self.save()
        self._unlock()

    def save(self):
        self._lock()
        self.data.strip_head()
        super(CKPT, self).save()
        self._unlock()

    def set(self, idx):
        self.data.set(idx, 1)

    def progress(self, sync=True):
        self._lock()

        if sync:
            self.save()
        self.data.strip_head()
        ret = self.data.get_fro()

        self._unlock()
        return ret

    def is_finished(self, length, sync=True):
        self._lock()
        if sync:
            self.save()

        self.data.strip_head()
        ret = self.data.get_fro() >= length

        self._unlock()
        return ret

    def unfinished_indices(self, length, sync=True, strict=True, list_or_iterable=True):
        self._lock()

        if sync:
            self.save()
        search_range = range(self.data.fro, min(self.data.to, length))
        gen = (idx for idx in search_range if (not self.data[idx])) if strict else (idx for idx in search_range)
        gen = chain(gen, range(min(self.data.to, length), length))
        ret = [i for i in gen] if list_or_iterable else gen

        self._unlock()
        return ret

    def merge(self, ckpt):
        self._lock()

        if isinstance(ckpt, SlidingList):
            self.data.merge(ckpt)
        else:
            ckpt = make_ckpt(ckpt)
            self.data.merge(ckpt.data)
        self.data.strip_head()

        self._unlock()

    def info(self):
        self._lock()

        ret = f"from={self.data.fro}, to={self.data.to}, data={str(list(self.data.data))}"

        self._unlock()
        return ret


class CKPTServer(DistributedDataServer):
    def __init__(self, addr=(socket.gethostbyname('localhost'), 65530)):
        super(CKPTServer, self).__init__(addr=addr, data_type=SlidingList)


def make_ckpt(ckpt):
    if ckpt is None:
        ret = CKPT()
    elif isinstance(ckpt, str):
        ret = CKPT(ckpt)
    elif isinstance(ckpt, CKPT):
        ret = ckpt
    else:
        raise TypeError()
    return ret


def __test_func(i):
    ckpt = CKPT('.ckpt', restart=False, save_every=1)
    ckpt[i] = True
    print(": ".join([str(os.getpid()), str(i), str(ckpt.data.fro), str(ckpt.data.to)]))


def _unit_test_local():
    import random
    import multiprocessing.pool as pool
    n = 2000

    ckpt = CKPT('.ckpt', restart=True, save_every=1)
    for i in range(n):
        ckpt[i] = True

    ckpt = CKPT('.ckpt', restart=False, save_every=1)
    assert(ckpt.is_finished(n))
    assert(not ckpt.is_finished(n + 1))

    import multiprocessing
    pl = pool.Pool(multiprocessing.cpu_count())

    indexes = [i for i in range(n)]
    random.seed(0)
    random.shuffle(indexes)
    ckpt = CKPT('.ckpt', restart=True, save_every=1)
    pl.map(__test_func, [i for i in indexes])

    ckpt = CKPT('.ckpt', restart=False, save_every=1)
    assert (ckpt.is_finished(n))
    assert (not ckpt.is_finished(n + 1))

    n = n * 2

    indexes = [i for i in range(n)]
    random.shuffle(indexes)
    pl.map(__test_func, [i for i in indexes])

    ckpt = CKPT('.ckpt', restart=False, save_every=1)
    assert (ckpt.is_finished(n))
    assert (not ckpt.is_finished(n + 1))


def _unit_test_client(addr, indices, ckpt=".ckpt.client"):
    ckpt = CKPT(ckpt=ckpt, server_addr=addr, update_interval=2, print_info=True)

    for idx in indices:
        ckpt[idx] = True

    import time
    time.sleep(20)
    ckpt.close()


def _unit_test_server(addr):
    server = CKPTServer(addr=addr)
    server.start()


def _unit_test_remote():
    from utils import search_port
    CKPT('.ckpt').restart()
    CKPT('.ckpt.client').restart()
    CKPT('.ckpt.client.0').restart()
    CKPT('.ckpt.client.1').restart()

    addr = (socket.gethostbyname('localhost'), search_port(60000))
    n = 1000
    indices = [i for i in range(n)]
    import random
    random.seed(0)
    random.shuffle(indices)

    step = n // 5 + 1
    idx = 0

    import multiprocessing
    server = multiprocessing.Process(target=_unit_test_server, args=(addr,))
    client0 = multiprocessing.Process(target=_unit_test_client, args=(addr, indices[idx: min(idx+step, len(indices))], '.ckpt.client.0'))
    idx += step
    client1 = multiprocessing.Process(target=_unit_test_client, args=(None, indices[idx: min(idx+step, len(indices))], '.ckpt.client.0'))
    idx += step
    client2 = multiprocessing.Process(target=_unit_test_client, args=(addr, indices[idx: min(idx+step, len(indices))], '.ckpt.client.1'))
    idx += step
    client3 = multiprocessing.Process(target=_unit_test_client, args=(None, indices[idx: min(idx+step, len(indices))], '.ckpt.client.1'))
    idx += step
    client4 = multiprocessing.Process(target=_unit_test_client, args=(addr, indices[idx: min(idx+step, len(indices))], '.ckpt'))

    server.start()
    import time
    time.sleep(1)
    client0.start()
    time.sleep(1)
    client1.start()
    client2.start()
    client3.start()
    client4.start()

    client0.join()
    client1.join()
    client2.join()
    client3.join()
    client4.join()
    # server.join()

    ckpt = CKPT('.ckpt')
    indices = ckpt.unfinished_indices(n)
    assert(len(indices) == 0)
    assert(not ckpt.is_finished(n + 1))
    print("success!")


def unit_test():
    _unit_test_local()
    _unit_test_remote()


if __name__ == '__main__':
    unit_test()
