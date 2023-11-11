"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import click
import threading
import socket
import mapreduce.utils
from mapreduce.utils.servers import tcp_server, udp_server
from mapreduce.utils.servers import tcp_client



# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""
    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host, manager_port,
        )

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register_ack",
            "worker_host": "localhost",
            "worker_port": 6001,
        }
        LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port

        self.current_job = None
        
        self.signals = {"shutdown": False}

        self.threads = []

        tcp_thread = threading.Thread(target=tcp_server, args=(host, port, self.signals, self.handle_tcp))
        self.threads.append(tcp_thread)

        tcp_thread.start()
        time.sleep(1)

        self.send_register()
        self.run_job()

        tcp_thread.join()

    def send_register(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))

            message = {
                "message_type" : "register",
                "worker_host" : self.host,
                "worker_port" : self.port,
            }

            message = json.dumps(message)
            sock.sendall(message.encode('utf-8'))

    def run_jon(self):
        while not self.signals["shutdown"]:

            if current_job is not None:
                job = self.current_job
            else:
                continue

            # do job
            time.sleep(1)

            message = {
                "message_type": "finished",
                "task_id": current_job['task_id'],
                "worker_host": self.host,
                "worker_port": self.port
            }

            tcp_client(self.manager_host, self.manager_port, "finished", message)   


    def handle_tcp(self, msg):
        message_type = msg["message_type"]

        if message_type == "shutdown":
            self.signals["shutdown"] == True

        if message_type == "new_map_task":
            current_job = msg
            

@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
