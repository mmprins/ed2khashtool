#!/usr/bin/env python3
# _*_coding:utf-8_*_
import os
import sys
import hashlib
import binascii
from multiprocessing import Pool,Manager
from urllib import parse
ARGS=sys.argv
FILENAME=os.path.split(ARGS[1])[-1]
FILESIZE=0
CPU_COUNT=os.cpu_count()
CHUNKS_TOTAL_COUNT=0
HASH_NULL=hashlib.new('md4',b'').digest()
CHUNKS_SIZE=9728000
CHUNKS_MD4_DICT={}
def chunks_do_hash(chunk_tuple):
    '''chunk_tuple数据格式:(chunks块数据,chunks块编号,queue对象);
    非空取值chunks数据块，计算md4后加入queue队列。queue对象数据
    格式：{chunks块编号:chunks块hash值}'''
    chunk_tuple[2].put({chunk_tuple[1]:hashlib.new('md4',chunk_tuple[0]).digest()})
def chunks_split(fp):
    '''依赖模块：mainwork()。对文件对象进行chunks分割操作，按照ed2k协议
    要求，每个chunks分区大小为9728000字节。本函数根据系统cpu数量，生成对
    应长度的chunks原始数据list，文件太小导致chunks分块不足cpu数量时，继
    续填充为None。生成的list将用于主函数进程池p=Pool()批量生成子进程。'''
    chunk_bytes_list.clear()
    for i in range(CPU_COUNT):
        chunkbytes=fp.read(CHUNKS_SIZE)
        if chunkbytes:
            chunk_bytes_list.append(chunkbytes)
        else:#fp读结束后继续填充None,???
            #有可能会产生[b'xyz',None]和[None]两种特殊填充值list对象
            chunk_bytes_list.append(None)
            break#中断list填充操作，list长度将小于实际cpu数量
def queue_get(q):
    '''依赖模块：mainwork,chunks_do_hash。用于读取由chunks_do_hash函数填
    充生成的chunks块MD4 hash值并加入dict对象，数据格式：{chunks块编号:
    chunks块hash值}'''
    while not q.empty():
        CHUNKS_MD4_DICT.update(q.get())
def ed2k_hash(CHUNKS_MD4_DICT):
    '''依赖：所有chunks块hash完成，并生成完整的CHUNKS_MD4_DICT对象。按照
    ed2k协议，完成文件整体的hash值结果。
    ED2K HASH规则：
    1.空文件直接对b''做MD4 hash；
    2.文件小于CHUNKS_SIZE时，ED2K hash等于单个chunks块的MD4 hash结果；
    3.文件大小是CHUNKS_SIZE的整数倍时，所有chunks块的MD4 hash按chunks
    编号拼接，额外加上HASH_NULL的hash值，再次计算MD4 hash值；
    4.文件大于CHUNKS_SIZE且非整数倍时，所有chunks块的MD4 hash按chunks
    编号拼接，再次计算MD4 hash值。'''
    long_hash_bytes=b''
    if FILESIZE == 0:#空文件hash值,转换为str类型
        hash_hexstring=binascii.b2a_hex(HASH_NULL).decode()
    elif FILESIZE < CHUNKS_SIZE:#文件小于CHUNKS_SIZE
        hash_hexstring=binascii.b2a_hex(CHUNKS_MD4_DICT[0]).decode()
    else:
        for i in range(len(CHUNKS_MD4_DICT)):
            long_hash_bytes+=CHUNKS_MD4_DICT[i]#按编号拼接
        if FILESIZE % CHUNKS_SIZE == 0:
            total_hash=hashlib.new('md4',long_hash_bytes+HASH_NULL)
        else:
            total_hash=hashlib.new('md4',long_hash_bytes)
        hash_hexstring=total_hash.hexdigest()
    return hash_hexstring.upper()
def mainwork(filename):
    global CHUNKS_TOTAL_COUNT#chunks编号
    global FILESIZE
    global chunk_bytes_list
    with open(filename,'rb') as fp:
        chunk_bytes_list=[True]#初始化变量用于起始循环
        while chunk_bytes_list[0]:#直到fp读完，chunk_bytes_list被填充为None
            chunks_split(fp)#调用函数chunks_split并将重置chunk_bytes_list
            #根据chunks_split函数重置变量chunk_bytes_list的实际长度申请Pool
            p=Pool(len(chunk_bytes_list))
            for i in range(len(chunk_bytes_list)):
                if chunk_bytes_list[i]:#过滤掉list变量中出现的None值
                    p.apply_async(chunks_do_hash,args=((chunk_bytes_list[i],CHUNKS_TOTAL_COUNT,q),))
                    #每一个非空值的chunk_bytes_list的值增加一个
                    #CHUNKS_TOTAL_COUNT的编号计数。
                    CHUNKS_TOTAL_COUNT+=1
            p.close()
            p.join()#chunks_do_hash子进程全部完成后继续操作fp重新申请进程池
        FILESIZE=fp.tell()
if __name__ == '__main__':
    q=Manager().Queue()
    mainwork(ARGS[1])
    queue_get(q)
    hash_code=ed2k_hash(CHUNKS_MD4_DICT)
    #用parse.quote(FILENAME)将str类型进行encode操作，并转换成带%的十六进制文本
    print('ed2k://|file|%s|%d|%s|/'%(parse.quote(FILENAME),FILESIZE,hash_code))
