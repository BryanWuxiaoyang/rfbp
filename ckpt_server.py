import os
import socket

try:
    from ckpt import CKPTServer
except ImportError:
    from rfbp.ckpt import CKPTServer

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-host', required=False, default=socket.gethostbyname('localhost'))
    parser.add_argument('-port', required=False, type=int, default=65530)

    args = parser.parse_args()
    print(args)

    server = CKPTServer(addr=(args.host, args.port))
    server.start()

