#!/usr/bin/env python3
import random
import socket
import sys
import struct
import hashlib
from time import sleep

from scapy.all import IP, UDP, Ether, get_if_hwaddr, get_if_list, sendp, Raw

# server60
# dst_ip = "192.168.10.111"
# dst_mac = "b8:ce:f6:9a:02:56"

# server165
dst_ip = "192.168.10.123"
dst_mac = "b8:ce:f6:9a:00:6f"

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

def prepare_for_Chmod_token(path, operation, real_keydepth): # only work for chmod operation
    # hash the key, key is path
    key2 = string_to_md5_bytes(path)
    key1 = ''
    keydepth = '01'
    if real_keydepth == 10: # use its prefix as key
        print("prefix: ", path[:path.rfind('/')])
        key1 = string_to_md5_bytes(path[:path.rfind('/')])
        keydepth = '02'
    key_hex_representation_1 = ''.join(f'{byte:02x}' for byte in key1)
    key_hex_representation_2 = ''.join(f'{byte:02x}' for byte in key2)
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
    PUTpayload = ''
    if keydepth == '01':
        PUTpayload = bytes.fromhex(
            "0001" # operation type: 2 bytes
            + real_path_depth_hex  # keydepth
            + keydepth # keydepth
            + key_hex_representation_2 # key: 16 bytes
            + operation_length_hex_representation # operation_length: 2 bytes (at least 8 bytes value)
            + operation_hex_representation # metadata operation
            + padding
            + '11' * 2 # adjust to align with PUTREQ_SEQ
            + '01' * 1
            + path_length_hex_representation
            + path_hex_representation # no padding here
        )
    else:
        PUTpayload = bytes.fromhex(
            "0001" # operation type: 2 bytes
            + real_path_depth_hex  # keydepth
            + keydepth # keydepth
            + key_hex_representation_1 # key: 16 bytes
            + key_hex_representation_2 # key: 16 bytes
            + operation_length_hex_representation # operation_length: 2 bytes (at least 8 bytes value)
            + operation_hex_representation # metadata operation
            + padding
            + '11' * 2 # adjust to align with PUTREQ_SEQ
            + '01' * 2
            + path_length_hex_representation
            + path_hex_representation # no padding here
        )
    print(PUTpayload.hex())
    return PUTpayload


def prepare_for_PUT_token(path, operation):
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
        "0001" # operation type: 2 bytes
        + "0001" # keydepth
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

def prepare_for_mv_PUT_token(path1, path2, operation):
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
        "0001" # operation type: 2 bytes
        + "0001" # keydepth
        + key_hex_representation # key: 16 bytes
        + operation_length_hex_representation # operation_length: 2 bytes (at least 8 bytes value)
        + operation_hex_representation # metadata operation
        + padding
        + '11' * 2 # adjust to align with PUTREQ_SEQ
        + '00' * 1 # token
        + path1_length_hex_representation
        + path1_hex_representation # no padding here
        + path2_length_hex_representation
        + path2_hex_representation # no padding here
    )
    return PUTpayload


def prepare_touch_mkdir_for_PUT_token(path, operation):
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
        + "0001" # keydepth
        + key_hex_representation # key: 16 bytes
        + operation_length_hex_representation # operation_length: 2 bytes (at least 8 bytes value)
        + operation_hex_representation # metadata operation
        + padding
        + '11' * 2 # adjust to align with PUTREQ_SEQ
        + '01' * 1 # token
        + path_length_hex_representation
        + path_hex_representation # no padding here
    )
    return PUTpayload


def prepare_for_GET_token(path, path_depth):
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
    real_path_depth_hex = f"{path_depth:02x}"
    hashes_hex = hashes_hex[-path_depth:]
    # assemble the packet
    GETpayload = bytes.fromhex(
        "0030"  # operation type
        + real_path_depth_hex
        + real_path_depth_hex
        + ''.join(hashes_hex)  # key1, key2, key3, ...
        + '01' * path_depth
        + path_length_hex_representation
        + path_hex_representation
    )
    return GETpayload


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
    

### PUT REQUESTS 
    
def send_put_mv_request_token(path1, path2, operation): # path1: src, path2: dst
    PUTpayload = prepare_for_mv_PUT_token(path1, path2, operation)
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
    
def send_touch_mkdir_request_token(path, operation):
    PUTpayload = prepare_touch_mkdir_for_PUT_token(path, operation)
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

def send_put_request_token(path, operation):
    PUTpayload = prepare_for_PUT_token(path, operation)
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
    
def send_put_chmod_request_token(path, operation, real_keydepth):
    PUTpayload = prepare_for_Chmod_token(path, operation, real_keydepth)
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

### GET REQUESTS

def send_get_request_token(path,depth):
    GETpayload = prepare_for_GET_token(path,depth)
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



    
def main():
    
    # target_path = "/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0/mdtest_tree.1/mdtest_tree.5/mdtest_tree.21/mdtest_tree.85/mdtest_tree.341/mdtest_tree.1365/mdtest_tree.5461/mdtest_tree.21845/file.mdtest.shared.7995271"
    # send_get_request_token(target_path, 10)
    # return
    
    # target_path = "/#test-dir.0.d.8.n.31738265.b.5/mdtest_tree.0/mdtest_tree.4/mdtest_tree.21/mdtest_tree.109/mdtest_tree.548/mdtest_tree.2745/mdtest_tree.13730/file.mdtest.shared.3995435"
    
    target_path = "/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0/mdtest_tree.1/mdtest_tree.5/mdtest_tree.21"
    
    
    # warmup_test('/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0')
    # warmup_test('/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0/mdtest_tree.1')
    
    # sleep(3)
    
    # send_put_chmod_request_token('/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0/mdtest_tree.1/mdtest_tree.5/mdtest_tree.21', 'chmod777', 4)
    for i in range(20):
        send_get_request_token('/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0/mdtest_tree.1', 2)
    # send_get_request_token('/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0/mdtest_tree.2', 2)
    # send_get_request_token(target_path, 10)
    # send_get_request_token(target_path, 10)
    # send_get_request_token(target_path, 10)
    # send_get_request_token(target_path, 10)
    # send_get_request_token(target_path, 10)
    # send_put_chmod_request_token(target_path, 'chmod777', 10)
    # send_get_request_token(target_path, 10)
    # send_get_request_token(target_path, 10)
    # send_get_request_token(target_path, 10)
    
    # sleep(1)
    
    # send_put_chmod_request_token(target_path, 'chmod777', 10)
    # # send_put_mv_request('/input/output/1.txt', '/input/output/2.txt', 'mv')
    return
    
    
    send_get_request_token('/#test-dir.0.d.2.n.63999999.b.4/mdtest_tree.0/mdtest_tree.1/mdtest_tree.5/file.mdtest.shared.15238100',4)
    return 
    send_get_request_token('/#test-dir.0.d.2.n.63999999.b.4/mdtest_tree.0',1)
    send_get_request_token('/#test-dir.0.d.2.n.63999999.b.4/mdtest_tree.0/mdtest_tree.1',2)
    send_get_request_token('/#test-dir.0.d.2.n.63999999.b.4/mdtest_tree.0/mdtest_tree.1/mdtest_tree.5',3)
    send_get_request_token('/#test-dir.0.d.2.n.63999999.b.4/mdtest_tree.0/mdtest_tree.1/mdtest_tree.5/file.mdtest.shared.15238095',4)
    send_get_request_token('/#test-dir.0.d.2.n.63999999.b.4/mdtest_tree.0/mdtest_tree.1/mdtest_tree.5/file.mdtest.shared.15238096',4)

    return 
    warmup_test('/#test-dir.0.d.2.n.63999999.b.4/mdtest_tree.0')
    warmup_test('/#test-dir.0.d.2.n.63999999.b.4/mdtest_tree.0/mdtest_tree.1')
    warmup_test('/#test-dir.0.d.2.n.63999999.b.4/mdtest_tree.0/mdtest_tree.1/mdtest_tree.5')
    warmup_test('/#test-dir.0.d.2.n.63999999.b.4/mdtest_tree.0/mdtest_tree.1/mdtest_tree.5/file.mdtest.shared.15238095')
    
    sleep(3)
    send_get_request_token('/#test-dir.0.d.2.n.63999999.b.4/mdtest_tree.0',1)
    send_get_request_token('/#test-dir.0.d.2.n.63999999.b.4/mdtest_tree.0/mdtest_tree.1',2)
    send_get_request_token('/#test-dir.0.d.2.n.63999999.b.4/mdtest_tree.0/mdtest_tree.1/mdtest_tree.5',3)
    send_get_request_token('/#test-dir.0.d.2.n.63999999.b.4/mdtest_tree.0/mdtest_tree.1/mdtest_tree.5/file.mdtest.shared.15238095',4)
    send_get_request_token('/#test-dir.0.d.2.n.63999999.b.4/mdtest_tree.0/mdtest_tree.1/mdtest_tree.5/file.mdtest.shared.15238096',4)

    return 
    warmup_test('/')
    warmup_test('/input')
    warmup_test('/input/1.txt')
    warmup_test('/input/2.txt')
    warmup_test("/input/output")
    warmup_test('/input/output/1.txt')
    warmup_test('/input/output/2.txt')
    
    # sleep(3)
    
    send_put_mv_request_token('/input/1.txt', '/input/11.txt', 'mv')
    
    
    # sleep(3)
    
    send_put_mv_request_token('/input/11.txt', '/input/1.txt', 'mv')
    
    # sleep(3)
    
    send_put_mv_request_token('/input/output', '/input/test', 'mv')

    # sleep(3)
    
    send_put_mv_request_token('/input/test', '/input/output', 'mv')
    
    # sleep(3)
    
    warmup_test('/input/3.txt')
    warmup_test('/input/4.txt')
    warmup_test('/input/5.txt')
    warmup_test('/input/6.txt')
    warmup_test('/input/7.txt')
    warmup_test('/input/8.txt')



if __name__ == "__main__":
    main()
