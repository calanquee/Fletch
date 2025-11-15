#!/usr/bin/env python3
import random
import socket
import sys
import struct
import hashlib
from time import sleep

from scapy.all import IP, UDP, Ether, get_if_hwaddr, get_if_list, sendp, Raw

# dst_ip = "192.168.0.130"
# dst_mac = "10:70:fd:66:00:58"

# server60
dst_ip = "192.168.10.111"
dst_mac = "b8:ce:f6:9a:02:56"

# server165
# dst_ip = "192.168.10.123"
# dst_mac = "b8:ce:f6:9a:00:6f"

_iface ="ens6np0"
def get_if():
    ifs = get_if_list()
    iface = None  # "h1-eth0"
    for i in get_if_list():
        if _iface in i:
            iface = i
            break
    if not iface:
        print("Cannot find ens6np0 interface")
        exit(1)
    return iface


def string_to_md5_bytes(input_string):
    hash_object = hashlib.md5()
    hash_object.update(input_string.encode())
    md5_bytes = hash_object.digest()
    # print(type(md5_bytes))
    # print(md5_bytes)
    return md5_bytes[0:8]


def int_to_hex_2bytes(number):
    if 0 <= number <= 0xFFFF:
        return format(number, '04x')
    else:
        return "Error: Number out of range (0-65535)"

def depth_to_hex(depth):
    return f'{depth:04x}'

def depth_to_hex_2(depth):
    return f'{depth:02x}{depth:02x}'

def prepare_for_WARMUP(path):
    # hash the key, key is path
    key = string_to_md5_bytes(path)
    key_hex_representation = ''.join(f'{byte:02x}' for byte in key)
    path_length_hex_representation = int_to_hex_2bytes(len(path))
    path_hex_representation = ''.join(f"{ord(c):02x}" for c in path)
    WARMUPpayload = bytes.fromhex(
        "0000"
        + "0001"
        + key_hex_representation
        + '01'
        + path_length_hex_representation
        + path_hex_representation
    )
    return WARMUPpayload


### WARMUP REQUESTS

def warmup_test(path):
    WARMUPpayload = prepare_for_WARMUP(path)
    addr = socket.gethostbyname(dst_ip)
    iface = get_if()
    print("sending on interface %s to %s" % (iface, str(addr)))
    pkt = Ether(src=get_if_hwaddr(iface), dst=dst_mac)
    pkt = (
        pkt
        / IP(dst=addr)
        / UDP(dport=1152, sport=random.randint(49152, 65535))
        / Raw(load=WARMUPpayload)
    )
    sendp(pkt, iface=iface, verbose=False)


def prepare_for_Chmod(path, operation, real_keydepth): # only work for chmod operation
    # hash the key, key is path
    key = string_to_md5_bytes(path)
    key_hex_representation = ''.join(f'{byte:02x}' for byte in key)
    # prepare for value: metadata operation
    value_length = len(operation)
    operation_hex_representation = ''.join(f"{ord(c):02x}" for c in operation)
    padding = ''
    # adjust length and padding if necessary
    if value_length % 8 != 0:
        padding_length = 8 - (value_length % 8)  # calculate required padding
        padding = '00' * padding_length
        value_length += padding_length
    operation_length_hex_representation = int_to_hex_2bytes(value_length)
    # prepare for the real path payload
    path_length_hex_representation = int_to_hex_2bytes(len(path))
    path_hex_representation = ''.join(f"{ord(c):02x}" for c in path)
    real_path_depth_hex = f"{real_keydepth:02x}"
    # assemble the packet
    
    
    PUTpayload = bytes.fromhex(
        "0001" # operation type: 2 bytes
        + real_path_depth_hex  # keydepth
        + "01" # keydepth
        + key_hex_representation # key: 16 bytes
        + operation_length_hex_representation # operation_length: 2 bytes (at least 8 bytes value)
        + operation_hex_representation # metadata operation
        + padding
        + '7FB4' # adjust to align with PUTREQ_SEQ
        + '00' * 1
        + path_length_hex_representation
        + path_hex_representation # no padding here
    )
    return PUTpayload

def prepare_for_RM(path, operation):
    # hash the key, key is path
    key = string_to_md5_bytes(path)
    key_hex_representation = ''.join(f'{byte:02x}' for byte in key)
    # prepare for value: metadata operation
    value_length = len(operation)
    operation_hex_representation = ''.join(f"{ord(c):02x}" for c in operation)
    padding = ''
    # adjust length and padding if necessary
    if value_length % 8 != 0:
        padding_length = 8 - (value_length % 8)  # calculate required padding
        padding = '00' * padding_length
        value_length += padding_length
    operation_length_hex_representation = int_to_hex_2bytes(value_length)
    # prepare for the real path payload
    path_length_hex_representation = int_to_hex_2bytes(len(path))
    path_hex_representation = ''.join(f"{ord(c):02x}" for c in path)
    # assemble the packet
    PUTpayload = bytes.fromhex(
        "0021" # operation type: 2 bytes
        + "0001" # keydepth
        + key_hex_representation # key: 16 bytes
        + operation_length_hex_representation # operation_length: 2 bytes (at least 8 bytes value)
        + operation_hex_representation # metadata operation
        + padding
        + '11' * 2 # adjust to align with PUTREQ_SEQ
        + '00' * 1
        + path_length_hex_representation
        + path_hex_representation # no padding here
    )
    return PUTpayload


def prepare_for_mv_PUT(path1, path2, operation):
    # hash the key, key is path
    key = string_to_md5_bytes(path1)
    key_hex_representation = ''.join(f'{byte:02x}' for byte in key)
    # prepare for value: metadata operation
    value_length = len(operation)
    operation_hex_representation = ''.join(f"{ord(c):02x}" for c in operation)
    padding = ''
    # adjust length and padding if necessary
    if value_length % 8 != 0:
        padding_length = 8 - (value_length % 8)  # calculate required padding
        padding = '00' * padding_length
        value_length += padding_length
    operation_length_hex_representation = int_to_hex_2bytes(value_length)
    # prepare for the real path payload
    path1_length_hex_representation = int_to_hex_2bytes(len(path1))
    path1_hex_representation = ''.join(f"{ord(c):02x}" for c in path1)
    path2_length_hex_representation = int_to_hex_2bytes(len(path2))
    path2_hex_representation = ''.join(f"{ord(c):02x}" for c in path2)
    # assemble the packet
    PUTpayload = bytes.fromhex(
        "0021" # operation type: 2 bytes
        + "0101" # keydepth
        + key_hex_representation # key: 16 bytes
        + operation_length_hex_representation # operation_length: 2 bytes (at least 8 bytes value)
        + operation_hex_representation # metadata operation
        + padding
        + '11' * 2 # adjust to align with PUTREQ_SEQ
        + '00' * 1
        + path1_length_hex_representation
        + path1_hex_representation # no padding here
        + path2_length_hex_representation
        + path2_hex_representation # no padding here
    )
    return PUTpayload

def prepare_touch_mkdir_for_PUT(path, operation):
    # hash the key, key is path
    key = string_to_md5_bytes(path)
    key_hex_representation = ''.join(f'{byte:02x}' for byte in key)
    # prepare for value: metadata operation
    value_length = len(operation)
    operation_hex_representation = ''.join(f"{ord(c):02x}" for c in operation)
    padding = ''
    # adjust length and padding if necessary
    if value_length % 8 != 0:
        padding_length = 8 - (value_length % 8)  # calculate required padding
        padding = '00' * padding_length
        value_length += padding_length
    operation_length_hex_representation = int_to_hex_2bytes(value_length)
    # prepare for the real path payload
    path_length_hex_representation = int_to_hex_2bytes(len(path))
    path_hex_representation = ''.join(f"{ord(c):02x}" for c in path)
    # assemble the packet
    PUTpayload = bytes.fromhex(
        "0011" # operation type: 2 bytes
        + "0101" # keydepth
        + key_hex_representation # key: 16 bytes
        + operation_length_hex_representation # operation_length: 2 bytes (at least 8 bytes value)
        + operation_hex_representation # metadata operation
        + padding
        + '11' * 2 # adjust to align with PUTREQ_SEQ
        + '01' * 1
        + path_length_hex_representation
        + path_hex_representation # no padding here
    )
    return PUTpayload



def prepare_for_GET(path, path_depth):
    # split path
    if not path.startswith('/'):
        path = '/' + path
    path_segments = path.strip('/').split('/')
    # compute hash values
    hashes_hex = []
    hash_bytes = string_to_md5_bytes('/')
    hashes_hex.append(''.join(f'{byte:02x}' for byte in hash_bytes))
    # if path_depth>1:
    for segment in path_segments:
        segment_path = '/' + '/'.join(path_segments[:path_segments.index(segment) + 1])
        hash_bytes = string_to_md5_bytes(segment_path)
        hashes_hex.append(''.join(f'{byte:02x}' for byte in hash_bytes))
    # represent the real path
    print(hashes_hex)
    # represent the real path
    path_length_hex_representation = int_to_hex_2bytes(len(path))
    path_hex_representation = ''.join(f"{ord(c):02x}" for c in path)
    
    hashes_hex = hashes_hex[-path_depth:]
    # assemble the packet
    GETpayload = bytes.fromhex(
        "0030"  # operation type
        + depth_to_hex_2(path_depth)  # keydepth
        + ''.join(hashes_hex)  # key1, key2, key3, ...
        + '01' * path_depth
        + path_length_hex_representation
        + path_hex_representation
    )
    # print(GETpayload.hex())
    return GETpayload


def send_get_request(path,depth):
    GETpayload = prepare_for_GET(path,depth)
    addr = socket.gethostbyname(dst_ip)
    iface = get_if()
    print("sending on interface %s to %s" % (iface, str(addr)))
    pkt = Ether(src=get_if_hwaddr(iface), dst=dst_mac)
    pkt = (
        pkt
        / IP(dst=addr)
        / UDP(dport=1152, sport=random.randint(49152, 65535))
        / Raw(load=GETpayload)
    )
    sendp(pkt, iface=iface, verbose=False)

def send_put_chmod_request(path, operation, real_keydepth):
    PUTpayload = prepare_for_Chmod(path, operation, real_keydepth)
    addr = socket.gethostbyname(dst_ip)
    iface = get_if()
    print("sending on interface %s to %s" % (iface, str(addr)))
    pkt = Ether(src=get_if_hwaddr(iface), dst=dst_mac)
    pkt = (
        pkt
        / IP(dst=addr)
        / UDP(dport=1152, sport=random.randint(49152, 65535))
        / Raw(load=PUTpayload)
    )
    sendp(pkt, iface=iface, verbose=False)
    
def send_put_rm_request(path, operation):
    PUTpayload = prepare_for_RM(path, operation)
    addr = socket.gethostbyname(dst_ip)
    iface = get_if()
    print("sending on interface %s to %s" % (iface, str(addr)))
    pkt = Ether(src=get_if_hwaddr(iface), dst=dst_mac)
    pkt = (
        pkt
        / IP(dst=addr)
        / UDP(dport=1152, sport=random.randint(49152, 65535))
        / Raw(load=PUTpayload)
    )
    sendp(pkt, iface=iface, verbose=False)


def send_put_mv_request(path1, path2, operation): # path1: src, path2: dst
    PUTpayload = prepare_for_mv_PUT(path1, path2, operation)
    addr = socket.gethostbyname(dst_ip)
    iface = get_if()
    print("sending on interface %s to %s" % (iface, str(addr)))
    pkt = Ether(src=get_if_hwaddr(iface), dst=dst_mac)
    pkt = (
        pkt
        / IP(dst=addr)
        / UDP(dport=1152, sport=random.randint(49152, 65535))
        / Raw(load=PUTpayload)
    )
    sendp(pkt, iface=iface, verbose=False)
    
def send_touch_mkdir_request(path, operation):
    PUTpayload = prepare_touch_mkdir_for_PUT(path, operation)
    addr = socket.gethostbyname(dst_ip)
    iface = get_if()
    print("sending on interface %s to %s" % (iface, str(addr)))
    pkt = Ether(src=get_if_hwaddr(iface), dst=dst_mac)
    pkt = (
        pkt
        / IP(dst=addr)
        / UDP(dport=1152, sport=random.randint(49152, 65535))
        / Raw(load=PUTpayload)
    )
    sendp(pkt, iface=iface, verbose=False)

def main():
    
    # warmup_test('/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0')
    # return 
    
    # send_get_request('/2/3/4', 3)
    # send_get_request('/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0/file.mdtest.shared.70', 2)
    send_get_request('/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0/mdtest_tree.2/mdtest_tree.12/mdtest_tree.49/mdtest_tree.199/mdtest_tree.800/mdtest_tree.3203/mdtest_tree.12813/mdtest_tree.51255/file.mdtest.shared.18759561', 10)
    return
    
    # # send_put_rm_request('/input/output/testdir','rmdir')
    send_put_chmod_request('/input/output/1.txt', 'chmod644', 4)
    # # send_put_mv_request('/input/output/1.txt', '/input/output/2.txt', 'mv')
    return
    
    # warmup
    # warmup_test('/')
    # warmup_test('/input')
    # warmup_test('/input/output')
    # warmup_test('/input/output/1.txt')
    # # warmup_test('/input/output/testdir')
    
    # sleep(5)
    # warmup_test('/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0')
    # warmup_test('/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0/mdtest_tree.4')
    # warmup_test('/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0/mdtest_tree.4/mdtest_tree.17')
    # warmup_test('/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0/mdtest_tree.4/mdtest_tree.17/mdtest_tree.71')
    # warmup_test('/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0/mdtest_tree.4/mdtest_tree.17/mdtest_tree.71/mdtest_tree.285')
    # warmup_test('/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0/mdtest_tree.4/mdtest_tree.17/mdtest_tree.71/mdtest_tree.285/mdtest_tree.1141')
    # warmup_test('/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0/mdtest_tree.4/mdtest_tree.17/mdtest_tree.71/mdtest_tree.285/mdtest_tree.1141/mdtest_tree.4567')
    # warmup_test('/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0/mdtest_tree.4/mdtest_tree.17/mdtest_tree.71/mdtest_tree.285/mdtest_tree.1141/mdtest_tree.4567/mdtest_tree.18272')
    # warmup_test('/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0/mdtest_tree.4/mdtest_tree.17/mdtest_tree.71/mdtest_tree.285/mdtest_tree.1141/mdtest_tree.4567/mdtest_tree.18272/mdtest_tree.73091')
    # warmup_test('/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0/mdtest_tree.4/mdtest_tree.17/mdtest_tree.71/mdtest_tree.285/mdtest_tree.1141/mdtest_tree.4567/mdtest_tree.18272/mdtest_tree.73091/file.mdtest.shared.26751506')
    # sleep(5)
    
    
    send_get_request('/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0/mdtest_tree.4', 2)
    send_get_request('/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0/mdtest_tree.4/mdtest_tree.17/mdtest_tree.71/mdtest_tree.285/mdtest_tree.1141/mdtest_tree.4567/mdtest_tree.18272/mdtest_tree.73091/file.mdtest.shared.26751506',10)
    # send_put_chmod_request('/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0', 'chmod777', 1)
    # send_get_request('/input/output/testdir', 4)
    
    return
    
    # examples
    # 1. mkdir
    send_touch_mkdir_request('/input','mkdir')
    # 2. touch
    send_touch_mkdir_request('/input/1.txt','touch')
    # 3. rm
    send_put_rm_request('/input/1.txt','rm')
    # 4. rmdir
    send_put_rm_request('/input','rmdir')
    # 5. mv/rename
    send_put_mv_request('/input/1.txt', '/input/2.txt', 'mv')
    # 6. chmod
    # if the target path's real_keydepth is 10, the key should be 9th key, because key9 and key10 shares a single lock (0a01)
    send_put_chmod_request('/input/2.txt', 'chmod777', 3) # 3 is the real_keydepth
    # add new test in mdtest

    # open close state
    # keydepth 0303
    # 7. GET
    send_get_request('/input/2.txt', 3) # in here, keydepth needs to be 0303, real_keydepth || keydepth
    
    return
    





if __name__ == "__main__":
    main()
