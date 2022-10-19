import os
import time

try:
    from ckpt import CKPT
except ImportError:
    from rfbp.ckpt import CKPT


if __name__ == '__main__':
    import argparse
    import socket
    import sys

    sys.path.append(os.getcwd())
    parser = argparse.ArgumentParser()
    parser.add_argument('-host', required=False, default=socket.gethostbyname('localhost'))
    parser.add_argument('-port', required=False, type=int, default=65530)
    parser.add_argument('-ckpt', required=False, default='.ckpt')
    parser.add_argument('-restart', required=False, type=bool, default=False)
    parser.add_argument('-update_interval', required=False, type=int, default=10)
    parser.add_argument('-print_info', required=False, type=bool, default=True)

    args = parser.parse_args()
    print(args)

    client = CKPT(ckpt=args.ckpt, server_addr=(args.host, args.port), restart=args.restart,
                  update_interval=args.update_interval, print_info=args.print_info)
    try:
        while True:
            time.sleep(60)
    except:
        client.close()
