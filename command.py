import os
import multiprocessing
import time
import signal

try:
    from rfbp_tools import Pool, get_context, set_context
except ImportError:
    from rfbp.rfbp_tools import Pool, get_context, set_context


def parse_command(command: str, print_command=True, is_async=True):
    if print_command:
        print(">>> ", command)
    command = command.strip()
    prev_proc = get_context('proc')

    if command.startswith("cd"):
        os.chdir(command.split()[1])
    elif command.startswith("exit"):
        return -1
    elif command.startswith("stop"):
        if prev_proc is not None:
            prev_proc.terminate()
            set_context(prev_proc=None)
    elif command.startswith("kill"):
        if prev_proc is not None:
            prev_proc.kill()
            set_context(prev_proc=None)
    else:
        if prev_proc is not None:
            prev_proc.join()
        proc = multiprocessing.get_context('fork').Process(target=os.system, args=(command,))
        proc.start()

        if is_async:
            set_context(prev_proc=proc)
        else:
            proc.join()
            set_context(prev_proc=None)


if __name__ == "__main__":
    import socket
    import argparse
    from rfbp_utils import local_ips

    parser = argparse.ArgumentParser()
    host = socket.gethostbyname(os.environ["ARNOLD_WORKER_0_HOST"] if os.environ.get("ARNOLD_WORKER_0_HOST") else socket.gethostname())
    parser.add_argument('-h', default=host)
    parser.add_argument('-p', type=int, default=65534)
    parser.add_argument('-s', type=bool, action='store_false')

    args = parser.parse_args()
    print(args)

    p = Pool((args.host, args.port))
    if args.server:
        p.start_server([], is_async=True)
        time.sleep(1)
        while True:
            command = input(">>> ").strip()
            p.append_args(args=[command], broadcast=True, is_async=True)
            try:
                parse_command(command, print_command=False, is_async=False)
            except KeyboardInterrupt:
                p.append_args(args=["stop"], broadcast=True, is_async=True)
                parse_command("stop", print_command=False, is_async=False)
    else:
        p.start_client(func=parse_command, stop_on_finished=False)
