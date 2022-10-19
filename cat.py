import sys


try:
    from sliding_list import SlidingList
    from rfbp_utils import lock_file_sh, unlock_file
except ImportError:
    from rfbp.sliding_list import SlidingList
    from rfbp.rfbp_utils import lock_file_sh, unlock_file


def cat(filename):
    with open(filename, "rb") as f:
        # lock_file_sh(f)
        slist = SlidingList().decode(f.read())
        # unlock_file(f)
        return f"from={slist.fro}, to={slist.to}, data={str(list(slist.data))}"


if __name__ == '__main__':
    filename = sys.argv[1]
    print(cat(filename))
