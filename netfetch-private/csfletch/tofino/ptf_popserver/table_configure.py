import os
import time
import json
import math
from itertools import product
import sys

import logging
import ptf
import grpc
from ptf import config
import ptf.testutils as testutils

from bfruntime_client_base_tests import BfRuntimeTest
import bfrt_grpc.bfruntime_pb2 as bfruntime_pb2
import bfrt_grpc.client as gc
import time
from time import sleep
from ptf.thriftutils import *
from ptf.testutils import *
from ptf_port import *
this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(this_dir))
from common import *

import threading
import socket
import struct

this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(this_dir))
from common import *

switchos_ptf_popserver_udpsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
switchos_ptf_popserver_udpsock.bind(("0.0.0.0", switchos_ptf_popserver_port))

# Qingxiu: cache_lookup1_tbl, cache_lookup2_tbl (for get and put cached)
# Qingxiu: cache_lookup3_tbl cache_lookup4_tbl (for lock)
CACHE_LOOKUP_TBL_SIZE = 16384 - 100
ROLLBACK = 25


class RegisterUpdate(BfRuntimeTest):
    def setUp(self):
        self.key_map = {} # Initialize empty dictionary.
        print('\nSetup')
        client_id = 1
        self.clean_period = 2
        p4_name = "csfletch"
        BfRuntimeTest.setUp(self, client_id, p4_name)
        bfrt_info = self.interface.bfrt_info_get("csfletch")
        
        self.target = gc.Target(device_id=0, pipe_id=0xffff)
        # Qingxiu: now we have three lookup tables
        self.cache_lookup1_tbl = bfrt_info.table_get("cache_lookup1_tbl")
        self.cache_lookup2_tbl = bfrt_info.table_get("cache_lookup2_1_tbl")
        self.cache_lookup3_tbl = bfrt_info.table_get("cache_lookup3_tbl")
        self.cache_lookup4_tbl = bfrt_info.table_get("cache_lookup4_tbl")
        self.cm1_reg = bfrt_info.table_get("cm1_reg")
        self.cm2_reg = bfrt_info.table_get("cm2_reg")
        self.cm3_reg = bfrt_info.table_get("cm3_reg")
        # Qingxiu: we have only one cache_frequency_reg, whose size is 3 times of the size of cache_lookup1_tbl
        self.cache_frequency_reg = bfrt_info.table_get("cache_frequency_reg")
        # lock counters
        self.reg_for_lock_key2 = bfrt_info.table_get("reg_for_lock_key2")
        self.reg_for_lock_key3 = bfrt_info.table_get("reg_for_lock_key3")
        self.reg_for_lock_key4 = bfrt_info.table_get("reg_for_lock_key4")
        self.reg_for_lock_key5 = bfrt_info.table_get("reg_for_lock_key5")
        self.reg_for_lock_key6 = bfrt_info.table_get("reg_for_lock_key6")
        self.reg_for_lock_key7 = bfrt_info.table_get("reg_for_lock_key7")
        self.reg_for_lock_key8 = bfrt_info.table_get("reg_for_lock_key8")
        self.reg_for_lock_key9 = bfrt_info.table_get("reg_for_lock_key9")

    def add_cache_lookup(self, token, keyhilo, keyhihilo, keyhihihi, freeidx, bitmap, latestidx, tbl_idx):
        bitmap_bits = ''.join(str((bitmap >> (15 - i)) & 1) for i in range(16))
        # print("key",hex(keyhilo), hex(keyhihilo), hex(keyhihihi),"token",token,"latestidx",latestidx)
        # print("Add key",latestidx, freeidx, bitmap_bits,"to cache_lookup{}_tbl".format(tbl_idx))

        ck_key = (keyhilo, keyhihilo, keyhihihi)

        if(tbl_idx==1):
            key = self.cache_lookup1_tbl.make_key([
                gc.KeyTuple('hdr.token_hdr.token', token),
                gc.KeyTuple('hdr.key1_hdr.keyhilo', keyhilo),
                gc.KeyTuple('hdr.key1_hdr.keyhihilo', keyhihilo),
                gc.KeyTuple('hdr.key1_hdr.keyhihihi', keyhihihi),])
            data = self.cache_lookup1_tbl.make_data(
                [gc.DataTuple('idx_for_latest_deleted_frequency_reg', latestidx), gc.DataTuple('idx', freeidx), gc.DataTuple('bitmap', bitmap)],
                'netcacheIngress.cached_action_tbl1')
            self.cache_lookup1_tbl.entry_add(self.target, [key], [data])
            if ck_key in self.key_map and self.key_map[ck_key] >=1:
                self.key_map[ck_key] += 1
                print("hash collision")
            else:
                self.key_map[ck_key] = 1
                key = self.cache_lookup3_tbl.make_key([
                    gc.KeyTuple('meta.keyhilo', keyhilo),
                    gc.KeyTuple('meta.keyhihilo', keyhihilo),
                    gc.KeyTuple('meta.keyhihihi', keyhihihi),])
                data = self.cache_lookup3_tbl.make_data(
                    [],
                    'netcacheIngress.cached_action_tbl3')
                self.cache_lookup3_tbl.entry_add(self.target, [key], [data])
        elif(tbl_idx==2):
            key = self.cache_lookup2_tbl.make_key([
                gc.KeyTuple('hdr.token_hdr.token', token),
                gc.KeyTuple('hdr.key1_hdr.keyhilo', keyhilo),
                gc.KeyTuple('hdr.key1_hdr.keyhihilo', keyhihilo),
                gc.KeyTuple('hdr.key1_hdr.keyhihihi', keyhihihi),])
            data = self.cache_lookup2_tbl.make_data(
                [gc.DataTuple('idx_for_latest_deleted_frequency_reg', latestidx), gc.DataTuple('idx', freeidx), gc.DataTuple('bitmap', bitmap)],
                'netcacheIngress.cached_action_tbl2')
            self.cache_lookup2_tbl.entry_add(self.target, [key], [data])
            if ck_key in self.key_map and self.key_map[ck_key] >=1:
                self.key_map[ck_key] += 1
                print("hash collision")
            else:
                self.key_map[ck_key] = 1
                key = self.cache_lookup4_tbl.make_key([
                    gc.KeyTuple('meta.keyhilo', keyhilo),
                    gc.KeyTuple('meta.keyhihilo', keyhihilo),
                    gc.KeyTuple('meta.keyhihihi', keyhihihi),])
                data = self.cache_lookup4_tbl.make_data(
                    [],
                    'netcacheIngress.cached_action_tbl4')
                self.cache_lookup4_tbl.entry_add(self.target, [key], [data])
        else:
            print("Invalid table index")
            exit(-1)

    def remove_cache_lookup(self, token, keyhilo, keyhihilo, keyhihihi, tbl_idx):
        # print("Remove key from cache_lookup{}_tbl".format(tbl_idx))
        ck_key = (keyhilo, keyhihilo, keyhihihi)
        self.key_map[ck_key] -= 1
        # print("remove key",hex(keyhilo), hex(keyhihilo), hex(keyhihihi),"token",token)
        # sys.stdout.flush()
        if(tbl_idx==1):
            key = self.cache_lookup1_tbl.make_key([
                gc.KeyTuple('hdr.token_hdr.token', token),
                gc.KeyTuple('hdr.key1_hdr.keyhilo', keyhilo),
                gc.KeyTuple('hdr.key1_hdr.keyhihilo', keyhihilo),
                gc.KeyTuple('hdr.key1_hdr.keyhihihi', keyhihihi),])
            self.cache_lookup1_tbl.entry_del(self.target, [key])
            if self.key_map[ck_key] == 0:
                key = self.cache_lookup3_tbl.make_key([
                    gc.KeyTuple('meta.keyhilo', keyhilo),
                    gc.KeyTuple('meta.keyhihilo', keyhihilo),
                    gc.KeyTuple('meta.keyhihihi', keyhihihi),])
                self.cache_lookup3_tbl.entry_del(self.target, [key])
        elif(tbl_idx==2):
            key = self.cache_lookup2_tbl.make_key([
                gc.KeyTuple('hdr.token_hdr.token', token),
                gc.KeyTuple('hdr.key1_hdr.keyhilo', keyhilo),
                gc.KeyTuple('hdr.key1_hdr.keyhihilo', keyhihilo),
                gc.KeyTuple('hdr.key1_hdr.keyhihihi', keyhihihi),])
            self.cache_lookup2_tbl.entry_del(self.target, [key])
            if self.key_map[ck_key] == 0:
                key = self.cache_lookup4_tbl.make_key([
                    gc.KeyTuple('meta.keyhilo', keyhilo),
                    gc.KeyTuple('meta.keyhihilo', keyhihilo),
                    gc.KeyTuple('meta.keyhihihi', keyhihihi),])
                self.cache_lookup4_tbl.entry_del(self.target, [key])
        else:
            print("Invalid table index")
            exit(-1)
    
    def cleaner(self):
        print("[cleaner start]")
        while True:
            sleep(self.clean_period)
            # start = time.time()
            self.cm1_reg.entry_del(self.target)
            self.cm2_reg.entry_del(self.target)
            self.cm3_reg.entry_del(self.target)
            # self.cache_frequency_reg.entry_del(self.target)

    def runpopserver(self):
        print("[ptf.popserver] ready")
        while True:
            # receive control packet
            recvbuf, switchos_addr = switchos_ptf_popserver_udpsock.recvfrom(1024)
            control_type, recvbuf = struct.unpack("=i{}s".format(len(recvbuf) - 4), recvbuf)
            # print(control_type)
            if control_type == SWITCHOS_ADD_CACHE_LOOKUP:
                # parse key and freeidx
                token, keyhilo, keyhihilo, keyhihihi, recvbuf = struct.unpack("!BI2H{}s".format(len(recvbuf)-9), recvbuf)
                freeidx, bitmap, latestidx = struct.unpack("=HHH", recvbuf)
                #freeidx, bitmap = struct.unpack("=HH", recvbuf)
                tbl_idx = 0
                if(latestidx // CACHE_LOOKUP_TBL_SIZE==0):
                    tbl_idx = 1
                else:
                    tbl_idx = 2
                self.add_cache_lookup(token, keyhilo, keyhihilo, keyhihihi, freeidx, bitmap, latestidx, tbl_idx)
                sendbuf = struct.pack("=i", SWITCHOS_ADD_CACHE_LOOKUP_ACK)
                switchos_ptf_popserver_udpsock.sendto(sendbuf, switchos_addr)
            elif control_type == SWITCHOS_REMOVE_CACHE_LOOKUP:
                # parse key
                token, keyhilo, keyhihilo, keyhihihi, recvbuf = struct.unpack("!BI2H{}s".format(len(recvbuf)-9), recvbuf)
                latestidx, = struct.unpack("=H", recvbuf[:2])
                if (latestidx // CACHE_LOOKUP_TBL_SIZE==0):
                    tbl_idx = 1
                else:
                    tbl_idx = 2
                # tbl_idx = (latestidx // CACHE_LOOKUP_TBL_SIZE) + 1
                self.remove_cache_lookup(token, keyhilo, keyhihilo, keyhihihi, tbl_idx)
                # send back SWITCHOS_REMOVE_CACHE_LOOKUP_ACK
                sendbuf = struct.pack("=i", SWITCHOS_REMOVE_CACHE_LOOKUP_ACK)
                switchos_ptf_popserver_udpsock.sendto(sendbuf, switchos_addr)
            elif control_type == SWITCHOS_PTF_POPSERVER_END:
                switchos_ptf_popserver_udpsock.close()
                print("[ptf.popserver] END")
                break
            elif control_type == ROLLBACK:
                print("start rollback lock counters")
                self.reg_for_lock_key2.entry_del(self.target)
                self.reg_for_lock_key3.entry_del(self.target)
                self.reg_for_lock_key4.entry_del(self.target)
                self.reg_for_lock_key5.entry_del(self.target)
                self.reg_for_lock_key6.entry_del(self.target)
                self.reg_for_lock_key7.entry_del(self.target)
                self.reg_for_lock_key8.entry_del(self.target)
                self.reg_for_lock_key9.entry_del(self.target)
            else:
                print("Invalid control type {}".format(control_type))
                exit(-1)
    
    def runTest(self):
        cleanert1 = threading.Thread(target=self.cleaner) 
        cleanert1.start() 
        popserver = threading.Thread(target=self.runpopserver) 
        popserver.start() 
        
        cleanert1.join()
        popserver.join() 