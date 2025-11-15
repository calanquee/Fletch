import os
import time
import json
import math
from itertools import product

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
switchos_ptf_popserver_udpsock.bind(("127.0.0.1", switchos_ptf_popserver_port))



class RegisterUpdate(BfRuntimeTest):

    def setUp(self):
        print('\nSetup')
        client_id = 2
        p4_name = "netfetch"
        BfRuntimeTest.setUp(self, client_id, p4_name)
        bfrt_info = self.interface.bfrt_info_get("netfetch")
        self.target = gc.Target(device_id=0, pipe_id=0xffff)
        self.reg_for_lock_key2 = bfrt_info.table_get("reg_for_lock_key4")

    def runpopserver(self):
        resp = self.reg_for_lock_key2.entry_get(self.target, None, {"from_hw": True})
        extracted_values = []
        data_dict = next(resp)[0].to_dict()
        for data, key in resp:
            pass
            data_dict = data.to_dict()
            key_dict = key.to_dict()
            extracted_value = data_dict.values()
            extracted_values.append(extracted_value)

        length = len(extracted_values)
        id = 0
        for value in extracted_values:
            id += 1
            print("id: %d, value: %s" % (id, value))
    
    def runTest(self):
        # cleanert1 = threading.Thread(target=self.cleaner) 
        # cleanert1.start() 
        popserver = threading.Thread(target=self.runpopserver) 
        popserver.start() 
        
        # cleanert1.join()
        popserver.join() 