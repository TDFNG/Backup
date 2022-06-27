import hashlib
import multiprocessing
import os
import random
import shutil
import threading
from queue import Queue


def getrelativepath(path: str, all_dir: list, all_file: list):
    rd = []
    rf = []
    for name in all_dir:
        rd.append(name.replace(path, '', 1))
    for name in all_file:
        rf.append(name.replace(path, '', 1))
    return rd, rf


def getcompletepath(path: str, all_dir: list, all_file: list):
    rd = []
    rf = []
    for name in all_dir:
        rd.append(path + name)
    for name in all_file:
        rf.append(path + name)
    return rd, rf


def hash_file(path: str):
    def read_chunks(file):
        while True:
            chunk = file.read(393216)
            if not chunk:
                break
            yield chunk

    h = hashlib.blake2b(usedforsecurity=False)
    with open(path, 'rb') as f:
        for block in read_chunks(f):
            h.update(block)
    return h.hexdigest()


def getalllist(path: str):
    all_dir = []
    all_file = []
    for root, dirs, files in os.walk(path, followlinks=False):
        for name in dirs:
            all_dir.append(os.path.join(root, name))
        for name in files:
            all_file.append(os.path.join(root, name))
    return all_dir, all_file


def getnewlist(opath: str, dpath: str, oad: list, oaf: list, dad: list, daf: list):
    oad, oaf = getrelativepath(opath, oad, oaf)
    dad, daf = getrelativepath(dpath, dad, daf)
    n_ad = list(set(oad) - set(dad))
    n_af = list(set(oaf) - set(daf))
    n_ad.sort()
    return getcompletepath(opath, n_ad, n_af)


def getdeletedlist(opath: str, dpath: str, oad: list, oaf: list, dad: list, daf: list):
    oad, oaf = getrelativepath(opath, oad, oaf)
    dad, daf = getrelativepath(dpath, dad, daf)
    d_ad = list(set(dad) - set(oad))
    d_af = list(set(daf) - set(oaf))
    d_ad.sort(reverse=True)
    return getcompletepath(dpath, d_ad, d_af)


def check_oq(oq, oaf):
    oh = []
    for path in oaf:
        oh.append(hash_file(path))
    oq.put(oh)


def check_dq(dq, daf):
    dh = []
    for path in daf:
        dh.append(hash_file(path))
    dq.put(dh)


def check(get, oaf, daf):
    x = []
    xl = []
    oq = Queue()
    dq = Queue()
    threading.Thread(target=check_oq, args=(oq, oaf)).start()
    threading.Thread(target=check_dq, args=(dq, daf)).start()
    oh = oq.get()
    dh = dq.get()
    for n in range(len(oh)):
        if oh[n] != dh[n]:
            x.append(n)
    for n in x:
        xl.append(oaf[n])
    get.put(xl)


def getchangedlist(opath: str, dpath: str, p):
    _ = [opath]
    _, p = getrelativepath(opath, _, p)
    random.shuffle(p)
    _, oaf = getcompletepath(opath, _, p)
    _, daf = getcompletepath(dpath, _, p)
    del _
    cn = multiprocessing.cpu_count()
    step = round(len(oaf) / cn) + 1
    oaf = [oaf[i:i + step] for i in range(0, len(oaf), step)]
    daf = [daf[i:i + step] for i in range(0, len(daf), step)]
    pool = multiprocessing.Pool(cn)
    get = multiprocessing.Manager().Queue()
    for i in range(len(oaf)):
        pool.apply_async(func=check, args=(get, oaf[i], daf[i]))
    pool.close()
    pool.join()
    xxl = []
    while get.qsize() > 0:
        for path in get.get():
            xxl.append(path)
    return xxl


def copy(get, path_all: list, opath: str, dpath: str):
    f = 0
    for path in path_all:
        try:
            shutil.copy(path, path.replace(opath, dpath, 1), follow_symlinks=False)
        except:
            f = f + 1
            print('无法同步文件：', path)
    get.put(f)


def io_copy(path: list, opath: str, dpath: str):
    cn = multiprocessing.cpu_count()
    step = round(len(opath) / cn) + 1
    random.shuffle(path)
    path = [path[i:i + step] for i in range(0, len(path), step)]
    pool = multiprocessing.Pool(cn)
    get = multiprocessing.Manager().Queue()
    for i in range(len(path)):
        pool.apply_async(func=copy, args=(get, path[i], opath, dpath))
    pool.close()
    pool.join()
    f = 0
    while get.qsize() > 0:
        f = f + get.get()
    return f


def delete(c, f, opath, dpath, oad, oaf, dad, daf):
    dd, df = getdeletedlist(opath, dpath, oad, oaf, dad, daf)
    c_n = len(dd) + len(df)
    f_n = 0
    for path in df:
        try:
            os.remove(path)
        except:
            f_n = f_n + 1
            print('无法删除文件：', path)
    for path in dd:
        try:
            shutil.rmtree(path)
        except:
            f_n = f_n + 1
            print('无法删除目录：', path)
    c.put(c_n)
    f.put(f_n)


def sync(opath: str, dpath: str, ch):
    f_n = 0
    oad, oaf = getalllist(opath)
    dad, daf = getalllist(dpath)
    c = multiprocessing.Queue()
    f = multiprocessing.Queue()
    p = multiprocessing.Process(target=delete, args=(c, f, opath, dpath, oad, oaf, dad, daf))
    p.start()
    nd, nf = getnewlist(opath, dpath, oad, oaf, dad, daf)
    c_n = len(nd) + len(nf)
    for path in nd:
        try:
            os.makedirs(path.replace(opath, dpath, 1))
        except:
            f_n = f_n + 1
            print('无法创建目录：', path)
    f_n = f_n + io_copy(nf, opath, dpath)
    if ch:
        cf = getchangedlist(opath, dpath, oaf)
        c_n = c_n + len(cf)
        f_n = f_n + io_copy(cf, opath, dpath)
    p.join()
    c_n = c_n + c.get()
    f_n = f_n + f.get()
    return c_n, f_n


if __name__ == "__main__":
    multiprocessing.freeze_support()
    spath = str(input('\n请输入主同步位置：\n\n'))
    mdpath = str(input('\n请输入辅同步位置：\n\n'))
    print('\n运行中...\n')
    C, F = sync(spath, mdpath, os.path.exists(mdpath))
    if F:
        print('\n', end='')
    print('总改动:', C, '个', '，失败:', F, '个', '，成功同步了:', C - F, '个', end='\n\n')
    os.system('pause')
