from datetime import datetime
from sh import findmnt
from sh import ls
from sh import mount
from sh import umount
from urllib.request import urlopen

import json
import os
import signal
import subprocess
import time

THREAD_SLEEP_IN_SECONDS = 60
DISKS_JSON_FILE_PATH = "disks.json"

CONTROLLER_ENABLED = True
NETWORK_WORKS = True

def handleSigInt(signalNumber, frame):
    global CONTROLLER_ENABLED
    CONTROLLER_ENABLED = False

class Logger:

    def __init__(self, wallet_log_path, controller_log_path):
        self.wallet_log_path = wallet_log_path
        self.controller_log_path = controller_log_path
        self.wallet_log("Controller started work")
        self.controller_log("Controller started work")
        print("Logger prepared ...")

    def __log(self, file, log):
        log_file = open(file, "a")
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        log_file.write("[" + dt_string + "]: " + log + "\n")
        log_file.close()
        
    def wallet_log(self, log):
        wallet_log_file = open(self.wallet_log_path, "a")
        self.__log(self.wallet_log_path, log)

    def controller_log(self, log):
        wallet_log_file = open(self.wallet_log_path, "a")
        self.__log(self.controller_log_path, log)

# --- END OF LOGGER

class LedCtrl:
    def __init__(self):
        pass

    def __turn_on(self):
        pass

    def __turn_off(self):
        pass

    def check_flags(self):
       pass

# --- END OF LED CONTROLLER

class Controller:

    # {"disk_uuid": {"mount_point", "name", "is_mounted"}}
    disks_mapping = {}

    def __init__(self, disks_mapping_file):
        self.logger = Logger("wallet.log", "controller.log")
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

        try:
            mounted_filesystems_json = json.loads(str(findmnt(mount_point, "-J")))
            filesystems_mounted_count = len(mounted_filesystems_json["filesystems"])
            self.logger.controller_log("Unmounting + " + mount_point + ". Found " + str(filesystems_mounted_count) + " filesystems mounted there.")
            for i in range(0, filesystems_mounted_count):
              umount(mount_point)
        except:
            self.logger.controller_log("Unmounting " + mount_point + " failed.")

    # - end of umount disk impl

    def __mount_disk(self, disk_id, mount_point):
        try:
            ls("/dev/disk/by-uuid/" + disk_id)
        except:
            self.logger.controller_log("Mounting " + disk_id + " to " + mount_point + " failed. Disk is not connected!")
            return

        try:
            mount("UUID="+ disk_id, mount_point)
            self.logger.controller_log("Mounted " + disk_id + ". Mount point: " + mount_point)
            self.disks_mapping[disk_id]["is_mounted"] = True
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

    def run(self):
        global CONTROLLER_ENABLED
        global THREAD_SLEEP_IN_SECONDS
        while CONTROLLER_ENABLED:
            self.__load_disks_mapping()
            self.__check_mount_points()
            self.__check_network()
            time.sleep(THREAD_SLEEP_IN_SECONDS)

# --- END OF CONTROLLER

if __name__ == '__main__':
    signal.signal(signal.SIGINT, handleSigInt)
    controller = Controller(DISKS_JSON_FILE_PATH)
    controller.run()