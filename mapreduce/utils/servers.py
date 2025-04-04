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
                    msg = json.loads(message_str)
                except json.JSONDecodeError:
                    continue 

                if msg["message_type"] == "shutdown":
                    signals["shutdown"] = True
                
                handle_func(msg)


def tcp_client(host, port, task="", message={}):

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))

        if task == "shutdown":
            message = json.dumps({"message_type": "shutdown"})
            sock.sendall(message.encode('utf-8'))
        
        elif task == "ack":
            message = json.dumps({"message_type": "register_ack"})
            sock.sendall(message.encode('utf-8'))       
        else:
            message = json.dumps(message)
            sock.sendall(message.encode('utf-8'))