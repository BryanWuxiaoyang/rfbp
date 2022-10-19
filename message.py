"""
A wrapper for raw sockets.
It enables user to send and receive independent and separated packages.
"""
import queue
import socket
import time


def expand_length(cur_len, exp_len):
    while cur_len < exp_len:
        cur_len = cur_len * 2 + 1
    return cur_len


def expand_bytearray(array: bytearray, array_view: memoryview,  new_len: int):
    array_len = len(array)
    if array_len < new_len:
        new_array = bytearray(new_len)
        new_array_view = memoryview(new_array)
        new_array_view[0: array_len] = array_view[:]
        array = new_array
        array_view = new_array_view
    return array, array_view


class MessageSocket:
    def __init__(self, sock, initial_send_len=1024, initial_recv_len=1024):
        self.sock = sock

        self.send_buf = bytearray(initial_send_len)
        self.send_buf_view = memoryview(self.send_buf)
        self.send_msg_len = 0
        self.send_offset = 0

        self.recv_buf = bytearray(initial_recv_len)
        self.recv_buf_view = memoryview(self.recv_buf)
        self.recv_msg_len = 0
        self.recv_offset = 0

    def send(self, payload: bytes, blocking=False):
        self.sock.setblocking(blocking)

        payload_len = len(payload)
        msg_len = 4 + payload_len
        # msg = int(payload_len).to_bytes(4, 'little', signed=False) + payload
        # self.sock.send(msg, flags)
        # return

        if len(self.send_buf) < msg_len:
            new_len = expand_length(len(self.send_buf), msg_len)
            self.send_buf, self.send_buf_view = expand_bytearray(self.send_buf, self.send_buf_view, new_len)
        # assert: send_buf is able to contain the whole message

        if len(self.send_buf) - self.send_offset < msg_len:
            self.send_buf_view[0: self.send_msg_len] = self.send_buf_view[self.send_offset: self.send_offset + self.send_msg_len]
            self.send_offset = 0
        # assert: send_buf[offset:] is able to contain the whole message

        new_offset = self.send_offset + msg_len
        self.send_buf_view[self.send_offset: self.send_offset + 4] = int(payload_len).to_bytes(4, 'little', signed=False)
        self.send_buf_view[self.send_offset + 4: new_offset] = payload

        while True:
            try:
                self.sock.send(self.send_buf_view[self.send_offset: new_offset])
                break
            except BlockingIOError:
                time.sleep(1)

        self.send_offset = new_offset

    def recv(self, blocking=False, max_size=10):
        self.sock.setblocking(blocking)

        sock = self.sock
        msg = self.recv_buf
        msg_view = memoryview(msg)
        msg_len = self.recv_msg_len
        offset = self.recv_offset

        payloads = []
        payload_len = -1
        while True:
            try:
                remain_space = len(msg) - msg_len
                if remain_space == 0:
                    msg, msg_view = expand_bytearray(msg, msg_view, len(msg) * 2 + 1)
                    remain_space = len(msg) - msg_len
                # assert: msg[msg_len: ] are able to contain at least one byte
                length = sock.recv_into(msg_view[msg_len:], remain_space)
                msg_len += length
                if length == 0 and msg_len == 0:
                    break
            except BlockingIOError:
                break
            except ConnectionResetError:
                break

            while True:
                if payload_len != -1:
                    pass
                elif msg_len >= 4:
                    payload_len = int.from_bytes(msg_view[offset: offset + 4], 'little')
                else:
                    break
                assert (payload_len != -1)
                if msg_len >= 4 + payload_len:
                    payload = msg_view[offset + 4: offset + 4 + payload_len]
                    payloads.append(bytes(payload))

                    msg_len = msg_len - (4 + payload_len)
                    offset += 4 + payload_len
                    payload_len = -1
                else:
                    break
            # assert: all complete messages in [msg] are processed
            # assert: remaining data are in msg[offset:]
            if offset > 0:
                # msg = bytearray(msg)
                # msg_view = memoryview(msg)
                msg_view[0: msg_len] = msg_view[offset: offset + msg_len]
                offset = 0
            if len(payloads) >= max_size:
                break

        self.recv_buf = msg
        self.recv_buf_view = msg_view
        self.recv_offset = offset
        self.recv_msg_len = msg_len

        return payloads

    def close(self):
        self.sock.close()


def _unit_test_server(port, result: queue.Queue):
    hostip = socket.gethostbyname('localhost')
    sock = socket.socket()
    sock.bind((hostip, port))
    sock.listen()
    client, addr = sock.accept()
    client = MessageSocket(client, initial_send_len=0, initial_recv_len=0)
    empty_count = 0

    while empty_count < 10:
        msgs = client.recv(blocking=True)
        if len(msgs) == 0:
            empty_count += 1
            time.sleep(1)
        else:
            for msg in msgs:
                result.put_nowait(msg.decode())

    client.close()
    sock.close()


def _unit_test_client(port, messages: str):
    hostip = socket.gethostbyname('localhost')
    sock = socket.socket()
    sock.connect((hostip, port))
    sock = MessageSocket(sock, 0, 0)

    for msg in messages:
        sock.send(msg.encode(), blocking=True)

    time.sleep(10)
    sock.close()


def unit_test():
    from threading import Thread

    port = 65535
    n = 1000
    messages = [str(i) for i in range(2000000, 2000000+n)]
    result = queue.Queue()
    client = Thread(target=_unit_test_client, args=(port, messages))
    server = Thread(target=_unit_test_server, args=(port, result))
    server.start()
    time.sleep(1)
    client.start()

    server.join()
    client.join()

    idx = 0
    while not result.empty():
        message = messages[idx]
        msg = result.get()
        assert message.__eq__(msg)
        idx += 1


if __name__ == '__main__':
    unit_test()
