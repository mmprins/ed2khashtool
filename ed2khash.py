#!/usr/bin/env python3
# _*_coding:utf-8_*_
import os
import sys
import hashlib
import binascii
from multiprocessing import Pool,Manager
ARGS=sys.argv
FILENAME=os.path.split(ARGS[1])[-1]
FILESIZE=0
CPU_COUNT=os.cpu_count()
CHUNKS_TOTAL_COUNT=0
HASH_NULL=hashlib.new('md4',b'').digest()
CHUNKS_SIZE=9728000
CHUNKS_MD4_DICT={}
def do_hash(chunk_tuple):
    if chunk_tuple[0]:
        chunk_tuple[2].put({chunk_tuple[1]:hashlib.new('md4',chunk_tuple[0]).digest()})
def chunks_split(fp):
    chunk_bytes_list.clear()
    for i in range(CPU_COUNT):
        chunkbytes=fp.read(CHUNKS_SIZE)
        if chunkbytes:
            chunk_bytes_list.append(chunkbytes)
        else:
            chunk_bytes_list.append(False)
            break
def queue_get(q):
    while not q.empty():
        CHUNKS_MD4_DICT.update(q.get())
def ed2k_hash(chunks_CHUNKS_MD4_DICT):
    long_hash_bytes=b''
    if FILESIZE == 0:
        hash_hexstring=binascii.b2a_hex(HASH_NULL).decode()
    elif FILESIZE < CHUNKS_SIZE:
        hash_hexstring=binascii.b2a_hex(chunks_CHUNKS_MD4_DICT[0]).decode()
    else:
        for i in range(len(chunks_CHUNKS_MD4_DICT)):
            long_hash_bytes+=chunks_CHUNKS_MD4_DICT[i]
        if FILESIZE % CHUNKS_SIZE == 0:
            total_hash=hashlib.new('md4',long_hash_bytes+HASH_NULL)
        else:
            total_hash=hashlib.new('md4',long_hash_bytes)
        hash_hexstring=total_hash.hexdigest()
    print('HASHDATA=%s'%hash_hexstring.upper())
def mainwork(filename):
    global CHUNKS_TOTAL_COUNT
    global FILESIZE
    global chunk_bytes_list
    with open(filename,'rb') as fp:
        chunk_bytes_list=[True]
        while chunk_bytes_list[0]:
            chunks_split(fp)
            p=Pool(len(chunk_bytes_list))
            for i in range(len(chunk_bytes_list)):
                if chunk_bytes_list[i]:
                    p.apply_async(do_hash,args=((chunk_bytes_list[i],CHUNKS_TOTAL_COUNT,q),))
                    CHUNKS_TOTAL_COUNT+=1
            p.close()
            p.join()
        FILESIZE=fp.tell()
if __name__ == '__main__':
    q=Manager().Queue()
    mainwork(ARGS[1])
    queue_get(q)
    print('FILENAME=%s\nFILESIZE=%d\nCHUNKS_MD4_DICT=%s'%(FILENAME,FILESIZE,CHUNKS_MD4_DICT))
    ed2k_hash(CHUNKS_MD4_DICT)
