import multiprocessing
import multiprocessing as mp
import queue
import sys
import threading
import time
import os
from collections import deque, defaultdict
from multiprocessing.managers import SyncManager

try:
    from rfbp_utils import *
    from ckpt import make_ckpt, CKPT
    from rfbp_utils import search_port, call_func
except ImportError:
    from rfbp.rfbp_utils import *
    from rfbp.ckpt import make_ckpt, CKPT
    from rfbp.rfbp_utils import search_port, call_func

RETURN_FAIL = 0
RETURN_SUCC = 1
RETURN_STOP = 2

context = {}


def get_context(key: str):
    return context.get(key)


def set_context(**kwargs):
    context.update(kwargs)


def run(data_source, process_fn, indices=None, ckpt=None, init_fn=None, finish_fn=None):
    ckpt = make_ckpt(ckpt)
    set_context(ckpt=ckpt)

    if init_fn:
        init_fn()

    if indices is None:
        indices = range(len(data_source))
    assert indices is not None
    for idx in indices:
        if ckpt[idx]:
            continue
        source = data_source[idx]
        set_context(index=idx)
        ret = process_fn(*source) if isinstance(source, tuple) else process_fn(source)

        if ret is None:
            ckpt[idx] = True
        elif ret == RETURN_FAIL:
            pass
        elif ret == RETURN_SUCC:
            ckpt[idx] = True
        elif ret == RETURN_STOP:
            ckpt[idx] = True
            break
    ckpt.save()

    if finish_fn:
        finish_fn()


def run_subprocess(data_source, process_fn, ckpt=None, local_rank=0, local_size=1, world_rank=0, world_size=1,
                   init_fn=None, finish_fn=None):
    ckpt = make_ckpt(ckpt)
    # rank, rank_dim = make_rank([(local_rank, local_size), (world_rank, world_size)])
    # indices = ckpt.unfinished_indices(len(data_source))
    # indices = [indices[i] for i in divide_fn(0, len(indices), rank, rank_dim)]
    # indices = scatter(ckpt.progress(), len(data_source), rank, rank_dim, False)

    data_source = split_shard(data_source, world_rank, world_size)
    indices = scatter(0, len(data_source), local_rank, local_size)

    set_context(local_rank=local_rank, local_size=local_size, world_rank=world_rank, world_size=world_size)
    run(data_source, process_fn, indices, ckpt, init_fn, finish_fn)


def run_all(data_source, process_fn, ckpt=None, local_size=1, world_rank=0, world_size=1, is_async=False,
            init_fn=None, finish_fn=None):
    procs = []
    for local_rank in range(local_size):
        proc = mp.get_context('fork').Process(target=run_subprocess, args=(
            data_source, process_fn, ckpt, local_rank, local_size, world_rank, world_size,
            True, init_fn, finish_fn))
        procs.append(proc)

    if not is_async:
        for proc in procs:
            proc.join()
    return procs


class IdAllocator:
    def __init__(self, initial_len=64):
        self.next = [i + 1 for i in range(initial_len)]
        self.free_head = 0

    def _extend(self):
        self.next = [i + 1 for i in range(len(self.next) * 2 + 1)]

    def alloc(self):
        ret = self.free_head
        self.free_head = self.next[self.free_head]
        return ret

    def free(self, id):
        self.next[id] = self.free_head
        self.free_head = id


class Manager(SyncManager):
    pass


class ClientData:
    def __init__(self):
        self.tick = time.time()
        self.running_jobs = {}
        self.job_queue = deque()


class Job:
    def __init__(self, job_id, args, is_broadcast):
        self.id = job_id
        self.args = args
        self.is_broadcast = is_broadcast

    def __eq__(self, other):
        return self.id == other.id


class Work:
    STATE_FINISHED = -1

    STATE_NO_AVAILABLE_JOB = -2

    STATE_CLIENT_DISCONNECTED = -3

    def __init__(self, args, ckpt=None, max_idle=300, post_fn=None, finish_fn=None, finish_queue=None):
        self.post_fn = post_fn
        self.finish_fn = finish_fn
        self.finish_queue = finish_queue
        self.job_queue = deque()
        self.finish_size = 0
        self.ckpt = make_ckpt(ckpt)
        self.max_idle = max_idle
        self.clients = {}
        self.id_allocator = IdAllocator(128)
        self.cid_counter = 0
        self.lock = threading.RLock()
        self.running_num = 0
        self.job_id = 0
        self.results = []
        self.is_finished = False

        self.job_iter = iter(args)
        # for arg in args:
        #     if self._check(self.job_id):
        #         self.job_queue.append(Job(self.job_id, arg, False))
        #     self.job_id += 1

    def _check(self, job_id):
        return not (self.ckpt and self.ckpt[job_id])

    def append_jobs(self, jobs, broadcast=False):
        with self.lock:
            if broadcast:
                for client in self.clients.values():
                    for job in jobs:
                        client.job_queue.append(Job(self.job_id, job, True))
                        self.job_id += 1
            else:
                for job in jobs:
                    self.job_queue.append(Job(self.job_id, job, False))
                    self.job_id += 1

    def _get_job_from_queue(self, q: deque):
        job = None
        while job is None and len(q) > 0:
            _job = q.popleft()
            assert isinstance(_job, Job)
            if self._check(_job.id):
                job = _job
        return job

    def get_job(self, cid: int):
        with self.lock:
            if self.checkin(cid) is False:
                return Work.STATE_CLIENT_DISCONNECTED, None
            self.refresh_clients()
            assert self.checkin(cid)

            client = self.clients[cid]
            job = None

            if job is None:
                job = self._get_job_from_queue(client.job_queue)
            if job is None:
                job = self._get_job_from_queue(self.job_queue)
            while job is None:
                try:
                    arg = next(self.job_iter)
                    if arg is None:
                        return Work.STATE_NO_AVAILABLE_JOB, None
                    elif self._check(self.job_id):
                        job = Job(self.job_id, arg, False)
                        self.job_id += 1
                        break
                    else:
                        self.job_id += 1
                except:
                    break

            if job is None and len(self.job_queue) == 0:
                if self.running_num == 0:
                    ret = self.finish_fn() if self.finish_fn else None
                    if self.finish_queue:
                        self.finish_queue.put(ret)

                    self.finish_fn = None
                    self.finish_queue = None
                    self.is_finished = True
                    return Work.STATE_FINISHED, None
                else:
                    return Work.STATE_NO_AVAILABLE_JOB, None
            # assert: job_id is not finished yet(but may be under processing by some workers)

            self.clients[cid].running_jobs[job.id] = job
            self.running_num += 1
            # print(f"send job [{job.id}] to client [{cid}]")
            return job.id, job.args

    def finish_job(self, cid, job_id, return_value=None):
        with self.lock:
            if self.checkin(cid) is False:
                return

            self.clients[cid].running_jobs.pop(job_id)
            self.running_num -= 1
            call_func(self.post_fn, return_value)
            if self.ckpt:
                self.ckpt[job_id] = True

        # print(f"finish job: job_id=[{job_id}], cid=[{cid}]")

    def refresh_clients(self):
        with self.lock:
            remove_list = []
            for cid, client in self.clients.items():
                tick = client.tick
                tock = time.time()
                if tock - tick > self.max_idle:
                    remove_list.append(cid)

            for cid in remove_list:
                self.checkout(cid)

    def checkin(self, cid):
        with self.lock:
            suc = False
            if self.clients.get(cid) is not None:
                self.clients[cid].tick = time.time()
                suc = True
            # print("checkin client: ", cid)
        return suc

    def register(self):
        with self.lock:
            # cid = self.id_allocator.alloc()
            cid = self.cid_counter
            self.cid_counter += 1
            self.clients[cid] = ClientData()

            print(f"register client: [{cid}]")
        return cid

    def checkout(self, cid):
        with self.lock:
            if self.clients.get(cid) is not None:
                jobs = self.clients[cid].running_jobs.values()
                self.running_num -= len(jobs)
                for job in jobs:
                    if not job.is_broadcast:
                        self.job_queue.appendleft(job)
                self.clients.pop(cid)
                # self.id_allocator.free(cid)

        print(f"checkout client: [{cid}]")

    def produce(self, value):
        with self.lock:
            self.results.append(value)

    def product(self):
        with self.lock:
            return self.results

    def execute(self, func, args):
        with self.lock:
            return call_func(func, args)

    def finished(self):
        with self.lock:
            return self.is_finished

    __work = None


class ServerStub:
    def __init__(self, args=None):
        self.args = args

    def initializer(self):
        pass

    def __iter__(self):
        return iter(self.args if self.args is not None else [])

    def post_fn(self, *args):
        pass

    def finish_fn(self):
        pass


class ClientStub:
    def initializer(self, rank: int, rank_dim: int):
        pass

    def func(self, *args):
        pass


def working_server(args, addr, chunk_size=1, ckpt=None, post_fn=None, finish_fn=None, finish_queue=None, init_fn=None, suc_queue=None):
    # args = [args[i: min(i + chunk_size, len(args))] for i in range(0, len(args), chunk_size)]
    try:
        if args is None:
            raise OSError()
        if init_fn:
            init_fn()
        Work.__work = Work(args, ckpt, post_fn=post_fn, finish_fn=finish_fn, finish_queue=finish_queue)
        Manager.register('get_work', lambda: Work.__work)
        manager = Manager(addr, authkey=b' ')
        if is_port_available(addr[1]):
            print("server start: ", addr)
            if suc_queue:
                suc_queue.put(True)
            manager.get_server().serve_forever()
        else:
            raise OSError()
    except OSError:
        print("server already started")
        if finish_queue:
            finish_queue.put(None)
        if suc_queue:
            suc_queue.put(False)


def get_client_manager(addr):
    Manager.register('get_work')
    manager = Manager(addr, authkey=b' ')
    return manager


def get_connected_client_manager(addr):
    manager = get_client_manager(addr)
    while True:
        try:
            manager.connect()
            break
        except ConnectionRefusedError:
            print("connection timeout, retry")
            time.sleep(5)
    return manager


def get_connected_work(addr) -> Work:
    manager = get_connected_client_manager(addr)
    work = manager.get_work()
    return work


def working_client(func, addr, ckpt=None, stop_on_finished=True, initializer=None, rank=0, rank_dim=1):
    ckpt = CKPT(ckpt, server_addr=addr) if ckpt else None

    class Value:
        def __init__(self, cid) -> None:
            self.value = cid

    work = get_connected_work(addr)
    cid = Value(-1)
    # print("try register")
    cid.value = work.register()
    finish = mp.Value('i', 0)
    lock = threading.Lock()
    print(f"[{cid.value}] client start")
    if initializer is not None:
        initializer(rank=rank, rank_dim=rank_dim)

    def refresh_t():
        suc = True
        while suc and finish.value == 0:
            time.sleep(10)
            try:
                with lock:
                    # print(f"[{cid.value}] try checkin")
                    suc = work.checkin(cid.value)
                    # print(f"[{cid.value}] end checkin")
            except:
                print("server closed")

        # print(f"[{cid.value}] stop checkin")

    refresh_process = threading.Thread(target=refresh_t, args=())
    refresh_process.start()
    try:
        try:
            while True:
                with lock:
                    # print(f"[{cid.value}] try getting job")
                    job_id, args_list = work.get_job(cid.value)
                    # print(f"[{cid.value}] end getting job")

                if job_id == Work.STATE_FINISHED:
                    # print(f"[{cid.value}] state finished")
                    if stop_on_finished:
                        break
                    else:
                        time.sleep(2)
                        continue
                elif job_id == Work.STATE_NO_AVAILABLE_JOB:
                    # print(f"[{cid.value}] no available job")
                    time.sleep(2)
                    continue
                elif job_id == Work.STATE_CLIENT_DISCONNECTED:
                    # print(f"[{cid.value}] client disconnected")
                    time.sleep(2)
                    cid.value = work.register()
                    continue

                set_context(job_id=job_id, ckpt=ckpt)
                # print(f"[{cid.value}] start job: [{job_id}]")
                ret_values = call_func(func, args_list)
                # print(f"[{cid.value}] end job: [{job_id}]")

                if ckpt:
                    ckpt[job_id] = True

                with lock:
                    # print(f"[{cid.value}] try finish job: [{job_id}]")
                    work.finish_job(cid.value, job_id, ret_values)
                    # print(f"[{cid.value}] finish job: [{job_id}]")
        except KeyboardInterrupt:
            pass

        with lock:
            work.checkout(cid.value)
    except:
        print("server closed")

    finish.value = 1
    refresh_process.join()
    print(f"[{cid.value}] checkout")


def working_clients(func, addr, processes=1, is_async=False, ckpt=None, stop_on_finished=True, initializer=None):
    if processes == 0:
        working_client(func, addr, ckpt, stop_on_finished, initializer)
    else:
        rank_dim = processes
        procs = []
        for rank in range(processes):
            proc = mp.get_context('spawn').Process(target=working_client, args=(
            func, addr, ckpt, stop_on_finished, initializer, rank, rank_dim))
            proc.start()
            procs.append(proc)
        if not is_async:
            for proc in procs:
                proc.join()
        return procs


class Pool:
    class Future:
        def __init__(self, finish_queue: multiprocessing.Queue, process=None):
            self.queue = finish_queue
            self.proc = process

        def get(self):
            ret = self.queue.get()
            if self.proc:
                time.sleep(5)
                self.proc.terminate()
            return ret

        def cancel(self):
            pass
            if self.proc:
                time.sleep(5)
                self.proc.terminate()

    def __init__(self, ip=socket.gethostbyname('localhost'), port=None):
        port = search_port() if port is None else port
        self.addr = (ip, port)

    def start_server(self, stub: ServerStub, ckpt=None, is_async=True):
        if not is_local_ip(self.addr[0]):
            return None

        args = stub
        init_fn = stub.initializer
        post_fn = stub.post_fn
        finish_fn = stub.finish_fn
        chunk_size = 1
        finish_queue = multiprocessing.Queue()
        suc_queue = multiprocessing.Queue()

        if not is_async:
            working_server(args, self.addr, chunk_size, ckpt, post_fn, finish_fn, finish_queue, init_fn, suc_queue)
            suc = suc_queue.get()
            future = Pool.Future(finish_queue, None) if suc else None
        else:
            proc = mp.get_context('fork').Process(target=working_server,
                                                  args=(args, self.addr, chunk_size, ckpt, post_fn, finish_fn, finish_queue, init_fn, suc_queue))
            proc.start()
            suc = suc_queue.get()
            future = Pool.Future(finish_queue, proc) if suc else None

        time.sleep(5)
        return future

    def start_client(self, stub: ClientStub, client_num=1, ckpt=None, is_async=False, stop_on_finished=True):
        func = stub.func
        initializer = stub.initializer
        procs = working_clients(func, self.addr, client_num, is_async, ckpt, stop_on_finished, initializer)
        return procs

    def start(self, server_stub: ServerStub, client_stub: ClientStub, client_num=1, ckpt=None, serial=False, is_async=True):
        if serial:
            return self.start_serial(server_stub, client_stub, client_num, ckpt)
        else:
            future = self.start_server(stub=server_stub, ckpt=ckpt, is_async=True)
            if client_num > 0:
                self.start_client(stub=client_stub, client_num=client_num, is_async=is_async)
            return future

    def start_serial(self, server_stub: ServerStub = None, client_stub: ClientStub = None, client_num=1, ckpt=None, is_async=True):
        ckpt = CKPT(ckpt) if ckpt else None
        server_stub.initializer()
        client_stub.initializer(rank=0, rank_dim=1)
        for i, args in enumerate(server_stub):
            if ckpt and ckpt[i]:
                continue
            args = call_func(client_stub.func, args)
            call_func(server_stub.post_fn, args)

        result = server_stub.finish_fn()
        q = multiprocessing.Queue()
        q.put(result)
        future = Pool.Future(q, None)
        return future

    def _append_args(self, args, chunk_size, broadcast):
        Manager.register('get_work')
        manager = Manager(self.addr, authkey=b' ')
        manager.connect()
        work = manager.get_work()
        args = [args[i: min(i + chunk_size, len(args))] for i in range(0, len(args), chunk_size)]
        work.append_jobs(args, broadcast)

    def append_args(self, args, chunk_size=10, broadcast=False, is_async=False):
        proc = mp.get_context('fork').Process(target=self._append_args, args=(args, chunk_size, broadcast))
        proc.start()
        if not is_async:
            proc.join()
        return proc


def working_client_yield(addr, ckpt=None, stop_on_finished=True, work=None, lock=None):
    ckpt = CKPT(ckpt, server_addr=addr) if ckpt else None

    class Value:
        def __init__(self, cid) -> None:
            self.value = cid

    work = get_connected_work(addr) if work is None else work
    lock = threading.Lock() if lock is None else lock
    cid = Value(-1)
    # print("try register")
    cid.value = work.register()
    finish = mp.Value('i', 0)
    print(f"[{cid.value}] client start")

    def refresh_t():
        suc = True
        while suc and finish.value == 0:
            time.sleep(10)
            try:
                with lock:
                    # print(f"[{cid.value}] try checkin")
                    suc = work.checkin(cid.value)
                    # print(f"[{cid.value}] end checkin")
            except:
                break
        print(f"[{cid.value}] stop checkin")

    refresh_process = threading.Thread(target=refresh_t, args=())
    refresh_process.start()
    try:
        try:
            while True:
                with lock:
                    # print(f"[{cid.value}] try getting job")
                    job_id, args_list = work.get_job(cid.value)
                    # print(f"[{cid.value}] end getting job")

                if job_id == Work.STATE_FINISHED:
                    print(f"[{cid.value}] state finished")
                    if stop_on_finished:
                        break
                    else:
                        time.sleep(2)
                        continue
                elif job_id == Work.STATE_NO_AVAILABLE_JOB:
                    # print(f"[{cid.value}] no available job")
                    time.sleep(2)
                    continue
                elif job_id == Work.STATE_CLIENT_DISCONNECTED:
                    print(f"[{cid.value}] client disconnected")
                    time.sleep(2)
                    cid.value = work.register()
                    continue

                set_context(job_id=job_id, ckpt=ckpt)
                yield args_list

                if ckpt:
                    ckpt[job_id] = True

                with lock:
                    # print(f"[{cid.value}] try finish job: [{job_id}]")
                    work.finish_job(cid.value, job_id, job_id)
                    # print(f"[{cid.value}] finish job: [{job_id}]")
        except KeyboardInterrupt:
            pass

        with lock:
            work.checkout(cid.value)
    except:
        print("server closed!")

    finish.value = 1
    refresh_process.join()
    print(f"[{cid.value}] checkout")


class ScheduledServer(ServerStub):
    def __init__(self, args, dependencies: list, ckpt=None):
        super(ScheduledServer, self).__init__(args if isinstance(args, list) else [arg for arg in args])
        self.graph = defaultdict(list)
        self.reverse_graph = defaultdict(list)
        for fro, to in dependencies:
            self.graph[fro].append(to)
            self.reverse_graph[to].append(fro)

        self.order = self._make_order()
        self.args = [self.args[self.order[i]] for i in range(len(self.args))]

        new_graph = defaultdict(list)
        new_reverse_graph = defaultdict(list)
        reverse_order = [None] * len(self.args)
        for i, o in enumerate(self.order):
            reverse_order[o] = i
        for fro, to in dependencies:
            new_graph[reverse_order[fro]].append(reverse_order[to])
            new_reverse_graph[reverse_order[to]].append(reverse_order[fro])
        self.graph = new_graph
        self.reverse_graph = new_reverse_graph

        self.ckpt = make_ckpt(ckpt)
        self.available_indices = queue.Queue()
        self.remain_counts = None
        self.sent_jobs = []

    def _make_order(self):
        remains = [len(self.reverse_graph[i]) for i in range(len(self.args))]
        avails = queue.Queue()
        order = []

        for i in range(len(self.args)):
            if remains[i] == 0:
                avails.put(i)

        while not avails.empty():
            i = avails.get()
            order.append(i)
            for next_i in self.graph[i]:
                remains[next_i] -= 1
                if remains[next_i] == 0:
                    avails.put(next_i)

        if not all(remains[i] == 0 for i in range(len(self.args))):
            raise RuntimeError("Error: detect cycle(s) in dependency graph")
        return order

    def __iter__(self):
        self.remain_counts = [len(self.reverse_graph[i]) for i in range(len(self.args))]
        for i in range(len(self.args)):
            if self.remain_counts[i] == 0:
                self.available_indices.put(i)

        while self.ckpt.progress() < len(self.args):
            found = None
            while not self.available_indices.empty():
                i = self.available_indices.get()
                if self.ckpt[i]:
                    self.post_fn(i)
                else:
                    found = i
                    break
            if found is None:
                yield None
            else:
                self.sent_jobs.append(found)
                yield self.args[found]

    def post_fn(self, job_id):
        i = self.sent_jobs[job_id]
        self.ckpt[i] = True
        for next_i in self.graph[i]:
            self.remain_counts[next_i] -= 1
            if self.remain_counts[next_i] == 0:
                self.available_indices.put(next_i)


class rfbp:
    def __init__(self, args, ip=socket.gethostbyname('localhost'), port=55553, ckpt=None, ckpt_server_only=True, chunk_size=1, serial=False, dependencies: list = None):
        time.sleep(random.random())
        self.args = args
        self.ip = ip
        self.port = port
        self.ckpt = ckpt
        self.ckpt_server_only = ckpt_server_only
        self.chunk_size = chunk_size
        self.serial = serial
        self.future = None
        self.results = None
        self.dependencies = dependencies

    def __iter__(self):
        args, ip, port, ckpt, ckpt_server_only, chunk_size = self.args, self.ip, self.port, self.ckpt, self.ckpt_server_only, self.chunk_size

        if self.serial:
            self.results = []
            if self.dependencies is not None and len(self.dependencies) > 0:
                server_stub = ScheduledServer(self.args, self.dependencies, self.ckpt)
                # print(server_stub.graph)
                # print(server_stub.reverse_graph)
                # print(server_stub.args)
                for i, arg in enumerate(server_stub):
                    yield arg
                    server_stub.post_fn(i)
                server_stub.finish_fn()
            else:
                ckpt = make_ckpt(ckpt)
                for i, arg in enumerate(args):
                    if ckpt[i]: continue
                    yield arg
        else:
            self.future = Pool(ip, port).start_server(stub=ServerStub(args), ckpt=ckpt, is_async=True) if self.dependencies is None else \
                Pool(ip, port).start_server(stub=ScheduledServer(self.args, self.dependencies, self.ckpt), ckpt=None, is_async=True)
            time.sleep(5)
            self.work = get_connected_work((ip, port))
            self.lock = threading.Lock()
            for arg in working_client_yield((ip, port), ckpt=None if ckpt_server_only else ckpt, work=self.work, lock=self.lock):
                yield arg
            if self.future:
                self.results = self.work.product()

    def produce(self, value):
        if self.serial:
            self.results.append(value)
        else:
            with self.lock:
                self.work.produce(value)

    def product(self):
        return self.results

    def execute(self, func, args):
        if self.serial:
            return call_func(func, args)
        else:
            return self.work.execute(func, args)

    def close(self):
        if self.future:
            self.future.cancel()


def mapping(data, func, ip=socket.gethostbyname('localhost'), port=63333, serial=False):
    data = rfbp(enumerate(data), ip=ip, port=port, serial=serial)
    for i, d in data:
        d = func(d)
        data.produce((i, d))
    ret = data.product()
    ret = [d[1] for d in ret]
    data.close()
    return ret


def _unit_test_func(i):
    # (i, f) = args
    CKPT('.ckpt')[i] = True


def _unit_test_run(args):
    run(args, _unit_test_func, ckpt=CKPT(restart=True, save_every=10))


def _unit_test_run_all(args):
    world_size = 4

    for world_rank in range(world_size):
        ckpt = CKPT('.ckpt', restart=True, save_every=10)
        run_all(args, _unit_test_func, local_size=multiprocessing.cpu_count(), world_rank=world_rank,
                world_size=world_size, ckpt=ckpt)


def _unit_test_run_subprocess(args):
    local_size = multiprocessing.cpu_count()
    world_size = 4
    procs = []

    for world_rank in range(world_size):
        ckpt = CKPT('.ckpt', restart=True, save_every=10)
        for local_rank in range(local_size):
            proc = mp.get_context('fork').Process(target=run_subprocess, args=(args, _unit_test_func, ckpt, local_rank,
                                                                               local_size, world_rank, world_size))
            proc.start()
    for proc in procs:
        proc.join()


def _unit_test_working(args):
    pass


def _unit_test(n, unit_test_fn, message):
    print(message)
    CKPT('.ckpt').restart()
    args = [i for i in range(n)]
    random.seed(0)
    random.shuffle(args)
    unit_test_fn(args)

    ckpt = CKPT('.ckpt')
    assert (ckpt.is_finished(n))
    assert (not ckpt.is_finished(n + 1))
    print("success!")


def unit_test():
    n = 1000
    _unit_test(n, _unit_test_working, 'test: working')
    # _unit_test(n, _unit_test_run, "test: run")
    # _unit_test(n, _unit_test_run_subprocess, "test: run_subprocess")
    # _unit_test(n, _unit_test_run_all, "test: run_all")
