import time, socket

try:
    import rfbp_tools
    import ckpt
except ImportError:
    import rfbp_tools
    import ckpt


def example_rfbp():
    """
    rfbp example: make a distributed program without even a single line of modification on code
    """
    for i in rfbp_tools.rfbp(range(30), port=55546, ckpt='ckpt'):
        time.sleep(1)
        print(i)


# stub example: using server_stub and client_stub to construct a multi-functional distributed program
class Server(rfbp_tools.ServerStub):
    def __init__(self, args):
        super(Server, self).__init__(args)
        self.results = []

    def __iter__(self):
        return super(Server, self).__iter__()

    def post_fn(self, i):
        self.results.append(i)
        print(f"post_fn {i}")

    def finish_fn(self):
        print("finish_fn")
        return self.results


class Client(rfbp_tools.ClientStub):
    def initializer(self, rank: int, rank_dim: int):
        print(f"initialize client: [{rank}]")

    def func(self, i):
        import time
        time.sleep(1)
        return i


def example_stub():
    """
    exaples for using stubs to make full use of the pool's functionality.
    """
    # create stubs
    server_stub, client_stub, client_num, cpt = Server(range(30)), Client(), 4, "ckpt"
    ckpt.CKPT(cpt).restart()  # comment this line if you want to resume from break point

    mode = ["single program", "single node", "multi-node", "serial"][2]
    if mode == "single program":
        # running like multiprocessing in a single parent process
        future = rfbp_tools.Pool().start(server_stub, client_stub, client_num, cpt)
    elif mode == "single node":
        # running on the single node, any client connected to this port will execute the functions
        future = rfbp_tools.Pool(port=65534).start(server_stub, client_stub, client_num, cpt)
    elif mode == "multi-node":
        # running in distributed environment, any client connected to this (ip, port) will execute the functions
        future = rfbp_tools.Pool(ip=socket.gethostbyname('localhost'), port=65535).start(server_stub, client_stub, client_num, cpt)
    elif mode == "serial":
        # running in serial version, used to easily debug your code
        future = rfbp_tools.Pool(ip=socket.gethostbyname('localhost'), port=65536).start_serial(server_stub, client_stub, client_num, cpt)
    else:
        raise RuntimeError()

    result = future.get()  # value returned by ServerStub().finish_fn() if this process holds the server
    print(result)


def example_stub_serial():
    # create stubs
    server_stub, client_stub, cpt = Server(range(30)), Client(), ckpt.CKPT('test.ckpt')
    cpt.restart()  # comment this line if you want to resume from break point

    client_stub.initializer(rank=0, rank_dim=1)  # on client process

    for i, arg in enumerate(server_stub):
        if cpt and cpt[i]: continue
        if isinstance(arg, tuple): ret = client_stub.func(*arg)  # on client process
        else: ret = client_stub.func(arg)

        server_stub.post_fn(ret)

    result = server_stub.finish_fn()
    return result


if __name__ == "__main__":
    example_rfbp()
    # example_stub()
    # example_stub_serial()
    pass
