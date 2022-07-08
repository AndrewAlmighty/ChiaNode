from datetime import datetime
from sh import findmnt, ls, mount, umount, pgrep
from urllib.request import urlopen

import glob
import json
import os
import requests
import signal
import subprocess
import time

# GLOBAL VARIABLES SECTION
BREAK_BETWEEN_JOBS_IN_SECONDS = 300

CONTROLLER_LOG_PATH="/root/controller.log"
DISKS_JSON_FILE_PATH = "/root/controller/disks.json"

RPC_REQUEST_HEADERS = {'Content-Type': 'application/json'}
BLOCKCHAIN_STATE_URL = "https://localhost:8555/get_blockchain_state"
CHIA_ROOT_DIR = os.getenv("CHIA_ROOT")  # shouldn't have '/' in the end.
FULL_NODE_CERT = (CHIA_ROOT_DIR + '/config/ssl/full_node/private_full_node.crt', CHIA_ROOT_DIR + '/config/ssl/full_node/private_full_node.key')

CHIA_NODE_PROCESS_NAME = "chia_full_node"
PATH_TO_RUN_CHIA_SCRIPT = "/root/controller/chia_full_node.sh"

CONTROLLER_ENABLED = True
NETWORK_WORKS = True
NODE_SYNCED = True
CHIA_NODE_ENABLED = True

# END OF GLOBAL VARIABLES SECTION

# SIGNAL HANDLERS

def handleSigInt(signalNumber, frame):
    global CONTROLLER_ENABLED
    CONTROLLER_ENABLED = False

def handleSigTerm(signalNumber, frame):
    global CONTROLLER_ENABLED
    CONTROLLER_ENABLED = False

# END OF SIGNALS HANDLERS

class Logger:

    def __init__(self, controller_log_path):
        #self.wallet_log_path = wallet_log_path
        self.controller_log_path = controller_log_path
        #self.wallet_log("Controller started work")
        self.controller_log("Controller started work")
        print("Logger prepared ...")

    def __log(self, file, log):
        log_file = open(file, "a")
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        log_file.write("[" + dt_string + "]: " + log + "\n")
        log_file.close()
        
    #def wallet_log(self, log):
    #    wallet_log_file = open(self.wallet_log_path, "a")
    #    self.__log(self.wallet_log_path, log)

    def controller_log(self, log):
        #wallet_log_file = open(self.wallet_log_path, "a")
        self.__log(self.controller_log_path, log)

# --- END OF LOGGER

class Controller:

    # {"disk_uuid": {"mount_point", "name", "is_mounted"}}
    disks_mapping = {}

    def __init__(self, disks_mapping_file):
        self.logger = Logger(CONTROLLER_LOG_PATH)
        self.disks_mapping_file = disks_mapping_file

    def __load_disks_mapping(self):
        mapping_file = open(self.disks_mapping_file, "r")
        mapping_json = json.loads(mapping_file.read())
        mapping_file.close()
        mapping_updated = False
        loaded_disk_ids = []

        for disk_json in mapping_json:
            disk_id = disk_json["disk_uuid"]
            loaded_disk_ids.append(disk_id)

            if disk_id not in self.disks_mapping.keys():
                disk_params = {"name": disk_json["name"], "mount_point": disk_json["mount_point"], "is_mounted": False}
                self.disks_mapping[disk_id] = disk_params
                mapping_updated = True
                self.__umount_disk(disk_id, disk_json["mount_point"])
            else:
                disk_params = self.disks_mapping[disk_id]
                if disk_params["name"] != disk_json["name"]:
                    self.disks_mapping[disk_id]["name"] = disk_json["name"]
                    mapping_updated = True
                if disk_params["mount_point"] != disk_json["mount_point"]:
                    self.disks_mapping[disk_id]["mount_point"] = disk_json["mount_point"]
                    self.__umount_disk(disk_id, disk_params["mount_point"])
                    self.__umount_disk(disk_id, disk_json["mount_point"])
                    mapping_updated = True
        
        disk_ids_to_remove = []
        for disk_id, disk_params in self.disks_mapping.items():
            if disk_id not in loaded_disk_ids:
                self.__umount_disk(disk_id, disk_params["mount_point"])
                disk_ids_to_remove.append(disk_id)

        for disk_id in disk_ids_to_remove:
            self.disks_mapping.pop(disk_id)
            mapping_updated = True

        if mapping_updated:
            self.logger.controller_log("Reloaded disks mapping\n" + str(self.disks_mapping) + "\n")

    # - end of load disks mapping impl

    def __umount_disk(self, disk_id, mount_point):
        self.disks_mapping[disk_id]["is_mounted"] = False

        if not os.path.isdir(mount_point):
            self.logger.controller_log("Unmounting " + mount_point + " failed. Mount point doesn't exists")
            return

        mounted_filesystems = ""

        try:
            mounted_filesystems = str(findmnt(mount_point, "-J"))
        except:
            self.logger.controller_log("No filesystems mounted on " + mount_point)
            return

        try:
            mounted_filesystems_json = json.loads(mounted_filesystems)
            filesystems_mounted_count = len(mounted_filesystems_json["filesystems"])
            self.logger.controller_log("Unmounting " + mount_point + ". Found " + str(filesystems_mounted_count) + " filesystems mounted there.")
            for i in range(0, filesystems_mounted_count):
              umount(mount_point)
        except:
            self.logger.controller_log("Unmounting " + mount_point + " failed.")

    # - end of umount disk impl

    def __mount_disk(self, disk_id, mount_point):
        if not os.path.isdir(mount_point):
            self.logger.controller_log("Mounting " + disk_id + " to " + mount_point + " failed. Mount point doesn't exists")
            return
            
        try:
            ls("/dev/disk/by-uuid/" + disk_id)
        except:
            self.logger.controller_log("Mounting " + disk_id + " to " + mount_point + " failed. Disk is not connected!")
            return

        try:
            ls("/dev/disk/by-uuid/" + disk_id)
        except:
            self.logger.controller_log("Mounting " + disk_id + " to " + mount_point + " failed. Disk is not connected!")
            return

        try:
            mount("UUID="+ disk_id, mount_point)
            self.disks_mapping[disk_id]["is_mounted"] = True
            plots_count = len(glob.glob1(mount_point, "*.plot"))
            self.logger.controller_log("Mounted " + disk_id + ". Mount point: " + mount_point + ". Plots count: " + str(plots_count))
        except:
            self.logger.controller_log("Mounting " + disk_id + " to " + mount_point + " failed.")

    # - end of mount disk impl

    def __check_mount_points(self):
        for disk_id, disk_params in self.disks_mapping.items():
            mount_point = disk_params["mount_point"]

            if disk_params["is_mounted"]:
                if len(os.listdir(mount_point)) == 0:
                    self.__umount_disk(disk_id, mount_point)
            else:
                if len(os.listdir(mount_point)) != 0:
                    self.logger.controller_log("Cannot mount disk " + disk_id + " to " + mount_point + " because point point contains files.")
                else:
                    self.__mount_disk(disk_id, mount_point)

    # - end of check mount points impl

    def __check_network(self):
        try:
          urlopen('http://www.google.com', timeout=1)
          global NETWORK_WORKS
          if not NETWORK_WORKS:
              NETWORK_WORKS = True
              self.logger.controller_log("Network connection restored.")

        except:
            if NETWORK_WORKS:
                NETWORK_WORKS = False
                self.logger.controller_log("Network connection has been lost.")
    
    # - end of check network impl

    def __check_blockchain_sync(self):
        try:
            response = json.loads(requests.post(BLOCKCHAIN_STATE_URL, data='{}', headers=RPC_REQUEST_HEADERS, cert=FULL_NODE_CERT, verify=False).text)
            is_synced = response["blockchain_state"]["sync"]["synced"]
            global NODE_SYNCED

            if not is_synced and NODE_SYNCED:
                self.logger.controller_log("Node is not synchronized with blockchain.")
                NODE_SYNCED = False
            elif is_synced and not NODE_SYNCED:
                NODE_SYNCED = True
                self.logger.controller_log("Node is synchronized with blockchain.")
        except requests.ConnectionError as error:
            self.logger.controller_log("Cannot send request to full node to check if blockchain is synced. Connection error: " + str(error))
        except requests.HTTPError as error:
            self.logger.controller_log("Cannot send request to full node to check if blockchain is synced. Http error: " + str(error))
        except:
            self.logger.controller_log("Cannot send request to full node to check if blockchain is synced. (Not known error)")

    # - end of check blockchain sync impl

    def __is_process_alive(self):
        global CHIA_NODE_ENABLED
        try:
            pgrep(CHIA_NODE_PROCESS_NAME)
            if not CHIA_NODE_ENABLED:
                CHIA_NODE_ENABLED = True
                self.logger.controller_log(CHIA_NODE_PROCESS_NAME + " process works again.")
        except:
            if CHIA_NODE_ENABLED:
                CHIA_NODE_ENABLED = False
                self.logger.controller_log(CHIA_NODE_PROCESS_NAME + " stopped working. Restarting ...")
                try:
                    output = subprocess.check_output(PATH_TO_RUN_CHIA_SCRIPT + " start", shell=True)
                    self.logger.controller_log(CHIA_NODE_PROCESS_NAME + " restarted. Output:\n" + str(output))
                    time.sleep(30) #let's wait a little to let process be ready to work.
                except subprocess.CalledProcessError as error:
                    self.logger.controller_log("Failed to run script: " + PATH_TO_RUN_CHIA_SCRIPT + ". Error: " + str(error))

    # - end of is_process_alive

    def run(self):
        global CONTROLLER_ENABLED
        global THREAD_SLEEP_IN_SECONDS
        last_time = 0
        while CONTROLLER_ENABLED:
            now = time.time()
            if now - last_time > BREAK_BETWEEN_JOBS_IN_SECONDS:
                self.__is_process_alive()
                self.__load_disks_mapping()
                self.__check_mount_points()
                self.__check_network()
                self.__check_blockchain_sync()
                last_time = time.time()
            time.sleep(10)

        self.logger.controller_log("Stopping " + CHIA_NODE_PROCESS_NAME + " ...")
        output = subprocess.check_output(PATH_TO_RUN_CHIA_SCRIPT, shell=True)
        self.logger.controller_log(CHIA_NODE_PROCESS_NAME + " stopped. Output:\n" + str(output))
        self.logger.controller_log("Controller is stopped with sigint or sigterm")

# --- END OF CONTROLLER

if __name__ == '__main__':
    signal.signal(signal.SIGINT, handleSigInt)
    signal.signal(signal.SIGTERM, handleSigTerm)
    controller = Controller(DISKS_JSON_FILE_PATH)
    controller.run()
