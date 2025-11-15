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

dst_ip = "192.168.0.110"
dst_mac = "b8:ce:f6:9a:02:57"

# dst_ip = "192.168.0.120"
# dst_mac = "b8:ce:f6:99:fe:07"
# dst_ip = "192.168.10.121"
# dst_mac = "b8:ce:f6:99:fe:06"
i_face = "ens3np0"
def get_if():
    ifs = get_if_list()
    iface = None  # "h1-eth0"
    for i in get_if_list():
        if i_face in i:
            iface = i
            break
    if not iface:
        print("Cannot find ens3np0 interface")
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


def prepare_for_PUT(path, operation):
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
        "0001" # operation type: 2 bytes
        + "0001" # keydepth
        + key_hex_representation # key: 16 bytes
        + operation_length_hex_representation # operation_length: 2 bytes (at least 8 bytes value)
        + operation_hex_representation # metadata operation
        + padding
        + '11' * 2 # adjust to align with PUTREQ_SEQ
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
        + "0001" # keydepth
        + key_hex_representation # key: 16 bytes
        + operation_length_hex_representation # operation_length: 2 bytes (at least 8 bytes value)
        + operation_hex_representation # metadata operation
        + padding
        + '11' * 2 # adjust to align with PUTREQ_SEQ
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
    # 
    hashes_hex = hashes_hex[-path_depth:]
    path_length_hex_representation = int_to_hex_2bytes(len(path))
    path_hex_representation = ''.join(f"{ord(c):02x}" for c in path)
    
    # assemble the packet
    GETpayload = bytes.fromhex(
        "0030"  # operation type
        + depth_to_hex(path_depth)  # keydepth
        + ''.join(hashes_hex)  # key1, key2, key3, ...
        + path_length_hex_representation
        + path_hex_representation
    )
    # print(GETpayload.hex())
    return GETpayload

def get_test(path):
    # hash the key, key is path
    key = string_to_md5_bytes(path)
    key_hex_representation = ''.join(f'{byte:02x}' for byte in key)
    # represent the real path
    path_length_hex_representation = int_to_hex_2bytes(len(path))
    path_hex_representation = ''.join(f"{ord(c):02x}" for c in path)
    # assemble the packet
    GETpayload = bytes.fromhex(
        "0030"  # operation type
        + "0001"  # keydepth
        + key_hex_representation  # key1, key2, key3, ...
        + path_length_hex_representation
        + path_hex_representation
    )
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
        + path_length_hex_representation
        + path_hex_representation
    )
    return WARMUPpayload

def WARMUP_SOME_PATHS(path):
    # split path
    if not path.startswith('/'):
        path = '/' + path
    path_segments = path.strip('/').split('/')
    path_depth = len(path_segments)
    WARMUPpayloadList = []
    WARMUPpayloadList.append(prepare_for_WARMUP('/'))
    print("/")
    for segment in path_segments:
        segment_path = '/' + '/'.join(path_segments[:path_segments.index(segment) + 1])
        print(segment_path)
        WARMUPpayloadList.append(prepare_for_WARMUP(segment_path))
    return WARMUPpayloadList, (path_depth+1)

def send_warmup_requests(path):
    WARMUPpayloadList,path_depth = WARMUP_SOME_PATHS(path)
    addr = socket.gethostbyname(dst_ip)
    iface = get_if()
    for i in range(path_depth):
        print("sending on interface %s to %s" % (iface, str(addr)))
        pkt = Ether(src=get_if_hwaddr(iface), dst=dst_mac)
        pkt = (
            pkt
            / IP(dst=addr)
            / UDP(dport=1152, sport=random.randint(49152, 65535))
            / Raw(load=WARMUPpayloadList[i])
        )
        sendp(pkt, iface=iface, verbose=False)
        sleep(1)

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

def send_get_request(path,depth):
    GETpayload = prepare_for_GET(path,depth)
    # print(GETpayload.hex())
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

def send_put_request(path, operation):
    PUTpayload = prepare_for_PUT(path, operation)
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
    warmup_test('/#test-dir.0.d.2.n.32760.b.4/mdtest_tree.0')
    warmup_test('/#test-dir.0.d.2.n.32760.b.4/mdtest_tree.0/mdtest_tree.1')
    warmup_test('/#test-dir.0.d.2.n.32760.b.4/mdtest_tree.0/mdtest_tree.1/mdtest_tree.5')
    warmup_test('/#test-dir.0.d.2.n.32760.b.4/mdtest_tree.0/mdtest_tree.1/mdtest_tree.5/file.mdtest.shared.9359')
    sleep(3)
    send_get_request('/#test-dir.0.d.2.n.32760.b.4/mdtest_tree.0',1)
    send_get_request('/#test-dir.0.d.2.n.32760.b.4/mdtest_tree.0/mdtest_tree.1',2)
    send_get_request('/#test-dir.0.d.2.n.32760.b.4/mdtest_tree.0/mdtest_tree.1/mdtest_tree.5',3)
    send_get_request('/#test-dir.0.d.2.n.32760.b.4/mdtest_tree.0/mdtest_tree.1/mdtest_tree.5/file.mdtest.shared.9359',3)
    return 
    # send_put_request('/input/1.txt','touch')
    # send_put_request('/input/2.txt','touch')
    # send_put_request('/input/3.txt','touch')
    # send_put_request('/input/4.txt','touch')
    # send_put_request('/input/5.txt','touch')
    # send_put_request('/input/6.txt','touch')
    # send_put_request('/input/7.txt','touch')
    # send_put_request('/input/8.txt','touch')
    # send_put_request('/input/9.txt','touch')
    # send_put_request('/input/10.txt','touch')
    
    # send_put_request('/output/2.txt','rm')
    
    # send_get_request("/input", 2)   
    # return 
    
    
    # send_put_request("/input/output/1.txt", 'chmod777')
    # return
    
    # for i in range(1):
    #     send_get_request('/input/5.txt', 3)
    
    # 1. send six warmup requests to saturate the regs
    
    # sleep(3)
    
    # send_put_request('/input/1.txt','rm')
    # send_touch_mkdir_request('/input/1.txt','touch')    
    
    warmup_test('/')
    warmup_test('/input')
    warmup_test('/input/1.txt')
    warmup_test('/input/2.txt')
    warmup_test("/input/output")
    warmup_test('/input/output/1.txt')
    warmup_test('/input/output/2.txt')
    
    send_put_mv_request('/input/1.txt', '/input/11.txt', 'mv')
    
    send_put_mv_request('/input/11.txt', '/input/1.txt', 'mv')
    
    send_put_mv_request('/input/output', '/input/test', 'mv')
    
    send_put_mv_request('/input/test', '/input/output', 'mv')
    
    send_put_request('/input/output','rmdir')
    
    send_touch_mkdir_request('/input/output','mkdir')
    send_touch_mkdir_request('/input/output/1.txt','touch')
    send_touch_mkdir_request('/input/output/2.txt','touch')    
    
    warmup_test('/input/3.txt')
    warmup_test('/input/4.txt')
    warmup_test('/input/5.txt')
    warmup_test('/input/6.txt')
    warmup_test('/input/7.txt')
    warmup_test('/input/output')
    
    # sleep(3)
    
    # send_get_request("/input/1.txt", 3)
    # send_put_request('/input/1.txt','chmod755')
    # send_get_request("/input/1.txt", 3)
    # sleep(3)
    # send_get_request("/input/1.txt", 3)
    
    
    
    # send_put_request('/input/1.txt','touch')
 
    # for i in range(4):
    #     path = f'/input/{i+1}.txt'
    #     for j in range(5):
    #         send_get_request(path, 3)

    # sleep(2)
    
    # # 2. send it to trigger eviction
    # for i in range(11):
    #     send_get_request("/input/output/test.txt", 4)
    
    # sleep(3)
    
    # send_get_request("/input/output/test.txt", 4)
    
    
    # warmup_test('/input/6.txt')
    
    # sleep(2)
    
    # warmup_test('/input/6.txt')
    
    # warmup_test('/input/7.txt')
    # warmup_test('/input/8.txt')
    # warmup_test('/input/9.txt')
    # warmup_test('/input/10.txt')
    
    # sleep(2)
    # for i in range(1):
    #     # send_get_request('/input/1.txt', 3)
    # get_test('/')
    # warmup_test('/input')
    
    # send_get_request('/input/11.txt', 3)
    # send_get_request('/input/2.txt', 3)


    # send_get_request('/input/test/3.txt', 4)
    # send_get_request('/input/1.txt', 1)
    # send_get_request('/input/7.txt', 3)
    return
    

    warmup_test('/')
    warmup_test('/input')
    warmup_test('/input/3.txt')
    warmup_test('/input/4.txt')
    warmup_test('/input/5.txt')
    warmup_test('/input/6.txt')
    warmup_test('/input/7.txt')
    # warmup_test('/input/8.txt')
    # warmup_test('/input/9.txt')
    # warmup_test('/input/10.txt')
    warmup_test("/input/output")
    warmup_test('/input/output/1.txt')
    warmup_test('/input/output/2.txt')    
    
    

    # sleep(5)

    # for i in range(5):
    #     send_get_request('/', 1)
    #     send_get_request('/input', 2)
    #     send_get_request('/input/3.txt', 3)
    #     send_get_request('/input/4.txt', 3)
    #     send_get_request('/input/5.txt', 3)
    #     send_get_request('/input/6.txt', 3)
    #     send_get_request('/input/7.txt', 3)
    #     send_get_request('/input/8.txt', 3)
    #     send_get_request('/input/9.txt', 3)
    #     send_get_request('/input/10.txt', 3)

    sleep(5)

    for times in range(10):
        if times == 0:
            for i in range(times+1):
                send_get_request('/input/output/2.txt', 4)
        if times == 1:
            for i in range(times+1):
                send_get_request('/input', 2)
        if times == 2:
            for i in range(times+1):
                send_get_request('/input/output/1.txt', 4)
        if times == 3:
            for i in range(times+1):
                send_get_request('/input/3.txt', 3)
        if times == 4:
            for i in range(times+1):
                send_get_request('/input/5.txt', 3)
        if times == 5:
            for i in range(times+1):
                send_get_request('/input/6.txt', 3)
        if times == 6:
            for i in range(times+1):
                send_get_request('/input/7.txt', 3)
        if times == 7:
            for i in range(times+1):
                send_get_request('/input/8.txt', 3)
        if times == 8:
            for i in range(times+1):
                send_get_request('/input/9.txt', 3)
        if times == 9:
            for i in range(times+1):
                send_get_request('/input/4.txt', 3)
                

    for i in range(41):
        send_get_request('/input/test/3.txt', 4)


    sleep(2)

    warmup_test('/input/1.txt')
    
    # sleep(1)

    # for i in range(11):
    #     get_test('/')

    # # sleep(1)



if __name__ == "__main__":
    main()
