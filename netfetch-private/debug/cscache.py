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

dst_ip = "192.168.10.121"
dst_mac = "b8:ce:f6:99:fe:06"
def get_if():
    ifs = get_if_list()
    iface = None  # "h1-eth0"
    for i in get_if_list():
        if "ens6np0" in i:
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
        + depth_to_hex(path_depth)  # keydepth
        + ''.join(hashes_hex)  # key1, key2, key3, ...
        + path_length_hex_representation
        + path_hex_representation
        + '00' # is_cached
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
    
    
def main():
    
    # send_put_mv_request("/input/output/2.txt", "/input/output/4.txt", "mv")
    # return
    
    # send_put_request("/", "ls")
    # return
    
    # get
    # send_put_request("/input/output/3.txt", "touch")
    # send_put_request("/input/output/2.txt", "touch")
    send_get_request("/input/output/2.txt", 1)
    return
    
    # touch
    send_put_request("/afterloading/afterloading", "mkdir")
    send_put_request("/input/output", "mkdir")
    # send_put_request("/input/output/3.txt", "touch")
    return
    
    # mkdir 
    send_put_request("/input/output/test", "mkdir")
    
    # chmod
    send_put_request("/input/output/1.txt", "chmod777")
    
    # mv
    send_put_mv_request("/input/output/2.txt", "/input/output/4.txt", "mv")
    
    # rm
    send_put_request("/input/output/4.txt", "rm")
    
    # rmdir
    send_put_request("/input/output/test", "rmdir")
    
    return
    
    
    return
    



if __name__ == "__main__":
    main()
