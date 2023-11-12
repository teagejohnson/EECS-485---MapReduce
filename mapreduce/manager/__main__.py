"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import click
import mapreduce.utils
import socket
import shutil
from queue import Queue, Empty
import threading
from mapreduce.utils.servers import tcp_server
from mapreduce.utils.servers import tcp_client


# Configure logging
LOGGER = logging.getLogger(__name__)

class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""

        self.host = host
        self.port = port

        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 6001,
        }
        LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        self.workers = []
        self.signals = {"shutdown": False}
        self.job_id = 0 # increment for each added worker
        self.job_queue = Queue()
        self.currently_running = False
        self.num_tasks = 0
        self.worker_jobs = {}


        self.threads = []
        tcp_thread = threading.Thread(target=tcp_server, args=(host, port, self.signals, self.handle_tcp))
        self.threads.append(tcp_thread)
        udp_thread = threading.Thread(target=self.udp_server)
        self.threads.append(udp_thread)
        heartbeat_thread = threading.Thread(target=self.heartbeart)
        self.threads.append(heartbeat_thread)



        tcp_thread.start()
        udp_thread.start()
        heartbeat_thread.start()
        time.sleep(1)

        self.run_job()

        tcp_thread.join()
        udp_thread.join()
        heartbeat_thread.join()


    def run_job(self):
        while not self.signals["shutdown"]:
            try:
                job = self.job_queue.get_nowait()
            except Empty:
                continue
            
            time.sleep(0.1)

            job_complete = False

            output_directory = job["output_directory"]
            id = job["job_id"]

            if os.path.exists(output_directory):
                shutil.rmtree(output_directory)

            os.mkdir(output_directory)

            # partition input files
            input_directory = job["input_directory"]
            files = [input_directory + '/' + f for f in os.listdir(input_directory)]
            files = sorted(files)

            num_mappers = job["num_mappers"]

            partitioned_files = []
            task_id = 0
            for i in range(num_mappers):
                partitioned_files.append((task_id, []))
                task_id = task_id + 1

            for i, file in enumerate(files):
                index = i % num_mappers
                partitioned_files[index][1].append(file)

            prefix = f"mapreduce-shared-job{id:05d}-"
            with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                LOGGER.info("Created tmpdir %s", tmpdir)

                self.num_tasks = len(partitioned_files)

                self.worker_jobs = {}

                while not self.signals["shutdown"] and not job_complete:
                    time.sleep(1)
                    if len(partitioned_files) > 0:
                        for worker in self.workers:
                            if worker["state"] == "ready":
                                message = {
                                    "message_type": "new_map_task",
                                    "task_id": partitioned_files[0][0],
                                    "input_paths": partitioned_files[0][1],
                                    "executable": job["mapper_executable"],
                                    "output_directory": tmpdir,
                                    "num_partitions": job["num_reducers"],
                                } 

                                worker["state"] == "busy"

                                tcp_client(worker['host'], worker['port'], task="new_map_task", message=message)
                                self.worker_jobs[worker['host'] + str(worker['port'])] = partitioned_files.pop(0)
                    
                    for worker in self.workers:
                        if worker['state'] == 'dead':
                            if worker['host'] + str(worker['port']) in self.worker_jobs.keys():
                                partitioned_files.append(self.worker_jobs[worker['host'] + str(worker['port'])])
                                del self.worker_jobs[worker['host'] + str(worker['port'])]
                    if self.num_tasks <= 0:
                        job_complete = True

                job_complete = False

                task_id = 0
                file_list = os.listdir(tmpdir)

                file_list = sorted(file_list)

                # partition
                partitioned_reduce = []
                task_id = 0
                for i in range(job['num_reducers']):
                    partitioned_reduce.append((task_id, []))
                    task_id = task_id + 1

                for file in file_list:
                    end = int(file[-5:])
                    partitioned_reduce[end][1].append(os.path.join(tmpdir, file))

                self.num_tasks = len(partitioned_reduce)

                self.worker_jobs = {}

                while not self.signals["shutdown"] and not job_complete:
                    time.sleep(1)

                    task_id = 0
                    file_list = os.listdir(tmpdir)

                    if len(partitioned_reduce) > 0:
                        for worker in self.workers:
                            if worker["state"] == "ready":
                                message = {
                                    "message_type": "new_reduce_task",
                                    "task_id": partitioned_reduce[0][0],
                                    "executable": job["reducer_executable"],
                                    "input_paths": partitioned_reduce[0][1],
                                    "output_directory": output_directory,
                                }

                                worker["state"] == "busy"

                                tcp_client(worker['host'], worker['port'], task="new_reduce_task", message=message)
                                self.worker_jobs[worker['host'] + str(worker['port'])] = partitioned_reduce.pop(0)

                    for worker in self.workers:
                        if worker['state'] == 'dead':
                            if worker['host'] + str(worker['port']) in self.worker_jobs.keys():
                                partitioned_reduce.append(self.worker_jobs[worker['host'] + str(worker['port'])])
                                del self.worker_jobs[worker['host'] + str(worker['port'])]
                    if self.num_tasks <= 0:
                        job_complete = True
            
            LOGGER.info("Cleaned up tmpdir %s", tmpdir)


    def udp_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            
            sock.settimeout(1)

            while not self.signals["shutdown"]:
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue

                message_str = message_bytes.decode("utf-8")
                message = json.loads(message_str)

                if message['message_type'] == "heartbeat":
                    for worker in self.workers:
                        if message['worker_host']  == worker['host'] and message['worker_port'] == worker['port']:
                            worker['heartbeat'] = True


    def heartbeart(self):
        while not self.signals["shutdown"]:
            time.sleep(10)

            for worker in self.workers:
                if worker['heartbeat'] == False:
                    worker['state'] = 'dead'

                elif worker['state'] == 'dead':
                    worker['state'] = 'ready'

                worker['heartbeat'] = False

            

    def handle_tcp(self, msg):
        message_type = msg["message_type"]

        if message_type == "shutdown":
            self.signals["shutdown"] == True
            
            # shutdown all workers
            for worker in self.workers:
                if worker['state'] != 'dead':
                    tcp_client(worker["host"], worker["port"], "shutdown")
        
        elif message_type == "register":
            worker = {
                "host": msg["worker_host"],
                "port": msg["worker_port"],
                "state": "ready",
                "heartbeat": False,
            }
            self.workers.append(worker)
            
            # send ack
            tcp_client(worker["host"], worker["port"], "ack")

        elif message_type == "new_manager_job":
            msg['job_id'] = self.job_id
            self.job_id = self.job_id + 1

            self.job_queue.put(msg)

        elif message_type == "finished":
            self.num_tasks = self.num_tasks - 1 
            for worker in self.workers:
                if worker['host'] == msg['worker_host'] and worker['port'] == msg['worker_port']:
                    worker['status'] = "ready"
                    del self.worker_jobs[worker['host'] + str(worker['port'])]
        

       




@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
