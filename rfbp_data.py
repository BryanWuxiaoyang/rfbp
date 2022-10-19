import multiprocessing
import threading
import time
from collections.abc import Generator
from multiprocessing.managers import SyncManager

try:
    from message import MessageSocket
    from rfbp_utils import lock_file_ex, lock_file_sh, unlock_file
except ImportError:
    from rfbp.message import MessageSocket
    from rfbp.rfbp_utils import lock_file_ex, lock_file_sh, unlock_file


class Data:
    def __init__(self):
        pass

    def encode(self) -> bytes:
        pass

    def decode(self, payload: bytes):
        pass

    def merge(self, other):
        other = other if isinstance(other, Data) else self.decode(other)
        return self._merge(other)

    def _merge(self, other):
        pass


class _DistributedData:
    def __init__(self, filepath=None, save_fn=None,
                 restart=False, broadcast_fn=None, receive_fn=None, print_info=False, data_type: Data = Data,
                 lock=None):
        self.filepath = filepath
        self.file = filepath
        self.save_count = 0
        self.save_fn = save_fn
        self.broadcast_fn = broadcast_fn
        self.receive_fn = receive_fn
        self.print_info = print_info
        self.closed = False
        self.data_type = data_type
        self.lock_object = lock

        if self.file:
            self.file = open(self.file, "ab+")
            self._lock_file_ex()

            self.file.seek(0)
            if restart:
                self.file.truncate(0)
                self.data = self.data_type()
                self._save_local()
            else:
                self.data = self._load_local()

            self._unlock_file()
        else:
            self.data = self.data_type()

    def restart(self):
        self.data = self.data_type()
        self._save_local()

    def _load_local(self):
        if self.file:
            self.file.seek(0)
            data = self.file.read()
            return self.data_type().decode(data)
        else:
            return None

    def _save_local(self):
        if self.file:
            self.file.truncate(0)
            self.file.seek(0)
            data = self.data.encode()
            self.file.write(data)
            self.file.flush()

    def _save_remote(self):
        if self.broadcast_fn is None:
            return
        self.broadcast_fn(self.data.encode())

    def _load_remote(self):
        if self.receive_fn is None:
            return []
        result = []
        recv_data = self.receive_fn()
        while recv_data:
            if isinstance(recv_data, list) or isinstance(recv_data, Generator):
                for recv in recv_data:
                    result.append(self.data_type().decode(recv))
                break
            else:
                result.append(self.data_type().decode(recv_data))
            recv_data = self.receive_fn()
        result.reverse()
        return result

    def _load(self):
        data = self._load_local()
        if data:
            self.data = self.data.merge(data)
        for recv in self._load_remote():
            self.data = self.data.merge(recv)

    def _save(self):
        self._load()
        self._save_remote()
        self._save_local()

    def _lock_file_ex(self):
        if self.lock_object:
            self.lock_object.acquire()
        elif self.file:
            lock_file_ex(self.file)

    def _lock_file_sh(self):
        if self.lock_object:
            self.lock_object.acquire()
        elif self.file:
            lock_file_sh(self.file)

    def _unlock_file(self):
        if self.lock_object:
            self.lock_object.release()
        elif self.file:
            unlock_file(self.file)

    def load(self):
        self._lock_file_sh()
        self._load()
        self._unlock_file()

    def save(self):
        self._lock_file_ex()
        self._save()
        self._unlock_file()
        self.save_count = 0
        if self.save_fn is not None:
            self.save_fn()

    def encode(self):
        raise NotImplemented()

    def decode(self, payload):
        raise NotImplemented()

    def close(self):
        if self.file:
            self.file.close()
        self.closed = True


# class DistributedDataServer:
#     def __init__(self, filepath='.distributed_data', restart=False, addr=(socket.gethostbyname('localhost'), 65530),
#                  update_interval=0, mode="active", close_wait=0, print_info=True, data_type: Data = Data, lock=None):
#         self.addr = addr
#         self.data = _DistributedData(filepath, restart=restart, data_type=data_type, lock=lock,
#                                      broadcast_fn=self.broadcast if mode == 'passive' else None,
#                                      receive_fn=self.receive if mode == 'passive' else None)
#         self.update_interval = update_interval
#         self.mode = mode
#         self.close_wait = close_wait
#         self.closed = False
#         self.print_info = print_info
#
#         assert (mode == 'active' and update_interval == 0)  # currently doesn't support 'passive' mode
#
#         self.server = socket.socket()
#         self.server.bind(addr)
#         self.server.listen()
#
#         self.client_threads = []
#         self.clients = []
#         self.lock = threading.Lock()
#
#         self.listener = threading.Thread(target=self.listen_t)
#         self.listener.start()
#
#         if self.update_interval > 0:
#             self.update_thread = threading.Thread(target=self.update_t)
#             self.update_thread.start()
#         else:
#             self.update_thread = None
#
#     def start(self):
#         print("server start: ", self.addr)
#         close_tick = -1
#         while True:
#             time.sleep(10)
#             if self.close_wait > 0:
#                 self.lock.acquire()
#                 if len(self.clients) == 0:
#                     if close_tick == -1:
#                         close_tick = time.time()
#                     else:
#                         close_tock = time.time()
#                         if close_tock - close_tick >= self.close_wait:
#                             self.lock.release()
#                             break
#                 else:
#                     close_tick = -1
#                 self.lock.release()
#
#         self.close()
#
#     def close(self):
#         # TODO: terminate threads
#         self.lock.acquire()
#
#         for client in self.clients:
#             client.close()
#         self.data.close()
#         self.server.close()
#         self.closed = True
#
#         self.lock.release()
#         print("server closed")
#
#     def listen_t(self):
#         while not self.closed:
#             try:
#                 client, addr = self.server.accept()
#             except ConnectionError:
#                 break
#             self.lock.acquire()
#             if self.closed:
#                 self.lock.release()
#                 break
#             print("server connected to client: ", addr)
#             client = MessageSocket(client)
#             self.clients.append(client)
#             if self.mode == 'active':
#                 index = len(self.clients) - 1
#                 thread = threading.Thread(target=self.client_t, args=(client, addr, index))
#                 self.client_threads.append(thread)
#                 thread.start()
#
#             self.lock.release()
#         print("listener stopped")
#
#
#     def receive(self):
#         msgs = []
#         for client in self.clients:
#             for msg in client.recv():
#                 msgs.append(msg)
#         return msgs
#
#     def broadcast(self, payload):
#         for client in self.clients:
#             client.send(payload)
#
#     def update_t(self):
#         while not self.closed:
#             time.sleep(self.update_interval)
#             self.lock.acquire()
#             if self.closed:
#                 self.lock.release()
#                 break
#             self.data.save()
#             self.lock.release()
#         print("updater stopped")
#
#     def client_t(self, client: MessageSocket, addr, index: int):
#         finish = False
#         while not finish:
#             assert not self.closed
#             msgs = client.recv(True, 3)
#             if len(msgs) == 0:
#                 break
#
#             slists = [self.data.data_type().decode(msg) for msg in msgs]
#             for slist in slists:
#                 print(f"receiving from {addr})")
#             self.lock.acquire()
#
#             for slist in slists:
#                 self.data.data.merge(slist)
#             self.data.save()
#
#             payload = self.data.data.encode()
#
#             self.lock.release()
#
#             for i in range(len(self.clients)):
#                 neighbour = self.clients[i]
#                 try:
#                     neighbour.send(payload)
#                 except BrokenPipeError:
#                     finish = True
#                     break
#
#         self.lock.acquire()
#
#         self.client_threads.remove(threading.currentThread())
#         self.clients.remove(client)
#         client.close()
#
#         self.lock.release()
#
#         print("server disconected with ", addr)

class DistributedManager(SyncManager):
    pass


class DistributedDataServer:
    def __init__(self, addr, data_type):
        self.addr = addr
        self.data = data_type()
        DistributedManager.register('get', lambda: self.data)
        self.manager = DistributedManager(address=addr)

    def _start(self, fault_tolerant=False):
        print("server started: ", self.addr)
        if fault_tolerant:
            try:
                self.manager.get_server().serve_forever()
            except OSError:
                print("port already used")
        else:
            self.manager.get_server().serve_forever()

    def start(self, fault_tolerant=False, is_async=False):
        if not is_async:
            self._start(fault_tolerant)
        else:
            proc = multiprocessing.get_context('fork').Process(target=self._start, args=(fault_tolerant,))
            proc.start()
            return proc


class DistributedData(_DistributedData):
    def __init__(self, filepath=None, save_fn=None, restart=False,
                 print_info=False, update_interval=15, server_addr=None, data_type: Data = Data, lock=None):
        _DistributedData.__init__(self, filepath=filepath, save_fn=save_fn, restart=restart,
                                  print_info=print_info, data_type=data_type,
                                  broadcast_fn=self.broadcast if server_addr else None,
                                  receive_fn=self.receive if server_addr else None,
                                  lock=lock)

        self.update_interval = update_interval
        self.closed = False

        # self.server = None
        # if is_local_master and server_addr and socket.gethostbyname('localhost') == server_addr[0]:
        #     if is_port_available(server_addr):
        #         # self.server = DistributedDataServer(filepath, restart=restart, addr=server_addr, close_wait=180)
        #         #
        #         # def server_t(serv):
        #         #     serv.start()
        #         #
        #         # server_thread = threading.Thread(target=server_t, args=(self.server,))
        #         # server_thread.start()
        #         self.server = DistributedManager(server_addr)
        #         multiprocessing.get_context('fork').Process(target=self.server.start, args=(True,)).start()

        self.tlock = None
        self.update_thread = None
        if server_addr:
            # self.server_addr = server_addr
            # self.sock = socket.socket()
            # if self.sock.connect_ex(server_addr) != 0:
            #     assert False
            # self.sock.setblocking(False)
            # self.sock = MessageSocket(self.sock)
            DistributedManager.register('get')
            manager = DistributedManager(address=server_addr)
            manager.connect()
            self.server_data = manager.get()
            print("client connected to server: ", server_addr)

            if update_interval > 0:
                self.tlock = threading.RLock()
                self.update_thread = threading.Thread(target=self.update_t)
                self.update_thread.start()

    def _lock(self):
        if self.tlock:
            self.tlock.acquire()

    def _unlock(self):
        if self.tlock:
            self.tlock.release()

    def broadcast(self, data):
        if self.print_info:
            print(f"client send ({self.data.fro}, {self.data.to})")
        # self.sock.send(data, False)
        if self.server_data:
            self.server_data.merge(data)

    def receive(self):
        if self.print_info:
            print(f"client recv")
        # return self.sock.recv(False)
        if self.server_data:
            return [self.server_data.encode()]
        else:
            return []

    def update_t(self):
        while not self.closed:
            time.sleep(self.update_interval)
            self.tlock.acquire()
            if self.closed:
                self.tlock.release()
                break

            self.save()
            # self.sock.send(payload)
            self.tlock.release()

    def restart(self):
        self._lock()
        ret = super(DistributedData, self).restart()
        self._unlock()
        return ret

    def load(self):
        self._lock()
        ret = super(DistributedData, self).load()
        self._unlock()
        return ret

    def save(self):
        self._lock()
        ret = super(DistributedData, self).save()
        self._unlock()
        return ret

    def close(self):
        self._lock()
        ret = super(DistributedData, self).close()
        # if self.server:
        #     self.server.close()
        self._unlock()
        return ret


