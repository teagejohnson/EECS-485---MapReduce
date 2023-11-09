"""TCP and UDP server code."""
import os
import tempfile
import logging
import json
import time
import click
import mapreduce.utils
import socket


def tcp_server(host, port, signals, handle_func):

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.listen()

            sock.settimeout(1)

            while not signals["shutdown"]: 
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                print("connection from", address[0])

                clientsocket.settimeout(1)

                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)

                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")

                try:
                    msg = json.loads(msg)
                except JSONDecodeError:
                    continue 
                
                handle_func(msg)


def udp_server(host, port, signals, handle_func):
        host = self.host
        port = self.port
        signals = self.signals

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            
            sock.settimeout(1)

            while not signals["shutdown"]:
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
            message_str = message_bytes.decode("utf-8")
            message_dict = json.loads(message_str)