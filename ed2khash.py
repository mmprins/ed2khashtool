#!/usr/bin/env python3
# _*_coding:utf-8_*_
import os
import sys
import hashlib
from multiprocessing import Pool
ARGS=sys.argv
CPU_COUNT=os.cpu_count()
TOTAL_COUNT=1
HASH_NULL=hashlib.new('md4',b'')
CHUNKS_COUNT=9728000
MD4_DICT={}
def do_hash(chunk_tuple):
    if chunk_tuple[0]:
        MD4_DICT[chunk_tuple[1]]=hashlib.new('md4',chunk_tuple[0]).digest()
def chunks_split(fp):
    chunk_bytes_list.clear()
    for i in range(CPU_COUNT):
        s=fp.read(CHUNKS_COUNT)
        if s:
            chunk_bytes_list.append(s)
        else:
            chunk_bytes_list.append(False)
            break
if __name__ == '__main__':
    with open(sys.argv[1],'rb') as fp:
        chunk_bytes_list=[True]
        while chunk_bytes_list[0]:
            chunks_split(fp)
            p=Pool(len(chunk_bytes_list))
            for i in range(len(chunk_bytes_list)):
                p.apply_async(do_hash,args=((chunk_bytes_list[i],TOTAL_COUNT),))
                TOTAL_COUNT+=1
            p.close()
            p.join()
        print('MD4_DICT=%s'%MD4_DICT)
