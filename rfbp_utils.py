import fcntl
import multiprocessing.pool
import random
import socket


def lock_file_ex(f):
    fcntl.lockf(f.fileno(), fcntl.LOCK_EX)


def lock_file_sh(f):
    fcntl.lockf(f.fileno(), fcntl.LOCK_EX)


def unlock_file(f):
    fcntl.lockf(f.fileno(), fcntl.LOCK_UN)


def split(offset, length, rank, rank_dim, list_or_iterable=True):
    share = length // rank_dim
    remain = length % rank_dim
    sub_offset = offset + share * rank + min(rank, remain)
    sub_length = (share + 1) if rank < remain else share
    it = range(sub_offset, sub_offset + sub_length)
    return [i for i in it] if list_or_iterable else it


def splits(offset, length, rank_dim, list_or_iterable=True):
    ret = []
    for rank in range(rank_dim):
        ret.append(split(offset, length, rank, rank_dim, list_or_iterable))
    return ret


def split_shard(inputs, rank, rank_dim, with_index=False, list_or_iterable=True):
    sub_indices = split(0, len(inputs), rank, rank_dim, list_or_iterable)
    sub_inputs = (inputs[idx] for idx in sub_indices)
    sub_inputs = [*sub_inputs] if list_or_iterable else sub_inputs
    if with_index:
        return sub_inputs, sub_indices
    else:
        return sub_inputs


def split_shards(inputs, rank_dim, with_index=False, list_or_iterable=True):
    ret = []
    for split_no in range(rank_dim):
        ret.append(split_shard(inputs, split_no, rank_dim, with_index, list_or_iterable))
    return ret


def scatter(offset, length, rank, rank_dim, list_or_iterable=True):
    it = range(offset + rank, length, rank_dim)
    return [i for i in it] if list_or_iterable else it


def scatters(offset, length, rank_dim, list_or_iterable=True):
    ret = []
    for rank in range(rank_dim):
        ret.append(scatter(offset, length, rank, rank_dim, list_or_iterable))
    return ret


def scatter_shard(inputs, rank, rank_dim, with_index=False, list_or_iterable=True):
    sub_indices = scatter(0, len(inputs), rank, rank_dim, list_or_iterable)
    sub_inputs = (inputs[idx] for idx in sub_indices)
    if with_index:
        return [sub_inputs], sub_indices if list_or_iterable else sub_inputs, sub_indices
    else:
        return [sub_inputs] if list_or_iterable else sub_inputs


def scatter_shards(inputs, rank_dim, with_index=False, list_or_iterable=True):
    ret = []
    for rank in range(rank_dim):
        ret.append(scatter_shard(inputs, rank, rank_dim, with_index, list_or_iterable))


def divide_shard(inputs, rank, rank_dim, mode='scatter', with_index=False, list_or_iterable=True):
    if mode == 'scatter':
        return scatter_shard(inputs, rank, rank_dim, with_index, list_or_iterable)
    elif mode == 'split':
        return split_shard(inputs, rank, rank_dim, with_index, list_or_iterable)
    else:
        raise TypeError()


def divide_shards(inputs, rank_dim, mode='scatter', with_index=False, list_or_iterable=True):
    if mode == 'scatter':
        return scatter_shards(inputs, rank_dim, with_index, list_or_iterable)
    elif mode == 'split':
        return split_shards(inputs, rank_dim, with_index, list_or_iterable)
    else:
        raise TypeError()


def make_rank(ranks):
    rank = 0
    rank_dim = 1

    for cur_rank, cur_rank_dim in ranks:
        rank += cur_rank * rank_dim
        rank_dim = cur_rank_dim * rank_dim
    return rank, rank_dim


class DefaultDivideStrategy:
    def __init__(self, mode='scatter', list_or_iterable=False):
        self.list_or_iterable = list_or_iterable
        if mode == 'scatter':
            self.divide_fn = scatter
        elif mode == 'split':
            self.divide_fn = split
        else:
            raise TypeError()

    def __call__(self, offset, length, rank, rank_dim):
        return self.divide_fn(offset, length, rank, rank_dim, self.list_or_iterable)


def shuffle(inputs, indices):
    datas = (inputs[idx] for idx in indices)
    new_indices = list(indices)
    random.shuffle(new_indices)
    for new_idx, data in zip(new_indices, datas):
        inputs[new_idx] = data


def _reduce_pair(data_source, reduce_fn, fetch_fn, index_pair):
    idx1, idx2 = index_pair
    data1, data2 = fetch_fn(data_source[idx1]), fetch_fn(data_source[idx2])
    return reduce_fn(data1, data2), index_pair


def _reduce_iter(data_source, reduce_fn, fetch_fn, indices, pool):
    fetch_fn = fetch_fn if fetch_fn else lambda v: v

    index_pairs = []
    index_pair = []
    for idx in indices:
        index_pair.append(idx)
        if len(index_pair) == 2:
            index_pairs.append(index_pair)
            index_pair = []

    next_indices = []
    if len(index_pair) > 0:
        assert len(index_pair) == 1
        data_source[index_pair[0]] = fetch_fn(index_pair[0])
        next_indices.append(index_pair[0])
        index_pair = None

    if pool:
        results = pool.map(_reduce_pair, ((data_source, reduce_fn, fetch_fn, index_pair) for index_pair in index_pairs))
        for data, index_pair in results:
            data_source[index_pair[0]] = data
            next_indices.append(index_pair[0])
    else:
        for index_pair in index_pairs:
            data, index_pair = _reduce_pair(data_source, reduce_fn, fetch_fn, index_pair)
            data_source[index_pair[0]] = data
            next_indices.append(index_pair[0])

    return next_indices


def reduce(data_source, reduce_fn, fetch_fn=lambda v: v, worker_size=0, in_place=False, rank=0, rank_dim=1):
    data_source = data_source if in_place else list(data_source)
    indices = DefaultDivideStrategy('scatter', False)(0, len(data_source), rank, rank_dim)
    pool = multiprocessing.pool.Pool(worker_size) if worker_size > 0 else None
    assert len(indices) > 0
    while len(indices) > 1:
        indices = _reduce_iter(data_source, reduce_fn, fetch_fn, indices, pool)

    return data_source[indices[0]]


def local_ips():
    import socket
    try:
        s = socket.getaddrinfo(socket.gethostname(), None)
        s = [i[4][0] for i in s]
    except:
        s = []
    return s + [socket.gethostbyname('localhost')]


def is_local_ip(ip):
    return ip in local_ips()


def is_port_available(port):
    import socket
    sock = socket.socket()
    try:
        sock.bind((socket.gethostbyname('localhost'), port))
        suc = True
    except OSError:
        suc = False
    finally:
        sock.close()
    return suc


def search_port(fro=50000, to=65536):
    for port in range(fro, to):
        if is_port_available(port):
            return port
    return -1


def call_func(func, args):
    if func is None:
        return
    if isinstance(args, tuple):
        return func(*args)
    else:
        return func(args)


