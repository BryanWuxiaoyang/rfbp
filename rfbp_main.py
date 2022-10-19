import bisect
import multiprocessing
import time
from collections import deque
try:
    from rfbp_tools import *
except ImportError:
    from rfbp.rfbp_tools import *


class Server(ServerStub):
    def initializer(self):
        self.args = [i for i in range(100)]

    def post_fn(self, i):
        print(i)

    def finish_fn(self):
        print("finish!")
        return 1


class Client(ClientStub):
    def initializer(self, rank, rank_dim):
        print(f"client init {rank}")

    def func(self, index):
        time.sleep(1)
        return index


def f(i):
    print(i)

if __name__ == '__main__':
    # main.py

    # 设置依赖图
    dependencies = []
    max_frames = 4189
    data = [i for i in range(max_frames)]
    restart_frames = [0, 405, 777, 1260, 1866, 2493, 3027, 3545, 4148]
    ranges = [
        (restart_frames[i], (restart_frames[i + 1] if i + 1 < len(restart_frames) else max_frames))
        for i in range(len(restart_frames))]

    for start, end in ranges:
        for i in range(start, end - 1):
            dependencies.append((i, i + 1))

    data = rfbp(data, ip='127.0.0.1', dependencies=dependencies, port=52360, serial=False)
    for i in data:
        data.execute(f, i)
        if i < 100: time.sleep(100)
        else: time.sleep(1)
    print(data.product())
    data.close()
