try:
    from rfbp_data import Data
except ImportError:
    from rfbp.rfbp_data import Data


class SlidingList(Data):
    def __init__(self, fro=0, to=0, data=None):
        super(SlidingList, self).__init__()
        if data is None:
            data = bytearray()
        self.data = data
        self.fro = fro
        self.to = to
        self.dirty = False
        self.payload = None

    def _reset_fro(self, fro):
        assert fro >= self.fro
        self.data = self.data[fro - self.fro:]
        self.fro = fro
        self.to = max(self.fro, self.to)

    def __getitem__(self, idx):
        if idx < self.fro:
            return True
        elif idx >= self.to:
            return False
        else:   
            return self.data[idx - self.fro] == 1

    def __setitem__(self, idx, value):
        if idx < self.fro:
            return

        if idx >= self.to:
            while self.to <= idx:
                self.data.append(0)
                self.to += 1
        # assert len(self.data) == self.to - self.fro
        self.data[idx - self.fro] = int(value)
        self.dirty = True

    def set(self, idx, value=1):
        self[idx] = value

    def get_fro(self):
        return self.fro

    def get_to(self):
        return self.to

    def get_data(self):
        return self.data

    def _merge(self, sparse_list):
        fro = max(self.fro, sparse_list.fro)
        self.data = self.data[min(len(self.data), fro - self.fro):]
        self.fro = fro
        self.to = max(self.to, fro)
        for idx in range(fro, sparse_list.to):
            data1, data2 = self[idx], sparse_list[idx]
            self[idx] = int(data1 or data2)
        self.dirty = True
        return self

    def strip_head(self, strip_value=1):
        idx = 0
        length = self.to - self.fro
        while idx < length and self.data[idx].__eq__(strip_value):
            idx += 1
        if idx > 0:
            self.data = self.data[idx:]
            self.fro += idx
            self.dirty = True

    def _encode(self):
        payload_len = 4 + 4 + len(self.data)
        payload = bytearray(payload_len)
        payload_view = memoryview(payload)
        payload_view[0: 4] = self.fro.to_bytes(4, 'little', signed=False)
        payload_view[4: 8] = self.to.to_bytes(4, 'little', signed=False)
        payload_view[8:] = self.data

        return payload

    def encode(self):
        if self.dirty or self.payload is None:
            self.payload = self._encode()
            self.dirty = False
        return self.payload

    def decode(self, payload):
        payload_view = memoryview(payload)
        self.fro = int.from_bytes(payload_view[0: 4], 'little', signed=False)
        self.to = int.from_bytes(payload_view[4: 8], 'little', signed=False)
        self.data = bytearray(payload_view[8:])
        return self

    def __eq__(self, other):
        return self.fro == other.fro and self.to == other.to and self.data.__eq__(other.data)


def unit_test():
    import random
    random.seed(0)
    n = 100000

    data = [i for i in range(n)]
    random.shuffle(data)

    slist = SlidingList()
    for d in data:
        slist[d] = True

    assert(slist.fro == 0)
    assert(slist.to == n)
    assert(len(slist.data) == n)

    code = slist.encode()
    slist2 = SlidingList.decode(code)
    assert(slist.fro == slist2.fro)
    assert(slist.to == slist2.to)
    assert(slist.data.__eq__(slist2.data))

    slist.strip_head(True)
    assert(slist.fro == n)
    assert(slist.to == n)
    assert(len(slist.data) == 0)

    n = n * 2
    data = [i for i in range(n)]
    random.shuffle(data)
    for d in data:
        slist[d] = True
        slist.strip_head(True)
    assert(slist.fro == n)
    assert(slist.to == n)
    assert(len(slist.data) == 0)


if __name__ == '__main__':
    unit_test()
