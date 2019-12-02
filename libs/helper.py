import os
import socket
import struct


def recvall(sock, n):
    # Helper function to recv n bytes or return None if EOF is hit
    data = b''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data


def recv_msg(sock):
    # Read message length and unpack it into an integer
    raw_msglen = recvall(sock, 4)
    if not raw_msglen:
        return None
    msglen = struct.unpack('>I', raw_msglen)[0]
    # Read the message data
    return recvall(sock, msglen)


def send_msg(sock, msg):
    # Prefix each message with a 4-byte length (network byte order)
    msg = struct.pack('>I', len(msg)) + msg
    sock.sendall(msg)


def send_file(sock, fp):

    data = fp.read(1024)
    print("Sending in progress..")
    while data:
        send_msg(sock, data)
        data = fp.read(1024)
    sock.close()


def recv_file(sock, fp):
    data = recv_msg(sock)
    while data:
        fp.write(data)
        data = recv_msg(sock)
    fp.close()
