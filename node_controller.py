from datetime import datetime
import json
import signal
import time

mount_point = "/mnt"

"""
    SIGUSR1 - mount disk
    SIGUSR2 - umount disk
    SIGRTMIN - reload disks mapping
"""

class Logger:

    def __init__(self, wallet_log_path, controller_log_path):
        self.wallet_log_path = wallet_log_path
        self.controller_log_path = controller_log_path
        self.wallet_log("Controller started work")
        self.controller_log("controller started work")

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

class Controller:

    # {"serial": {"mount_directory", "name", "partition_uuid"}}
    disks_mapping = {}

    def __init__(self, disk_mapping_file):
        self.enabled = True
        self.logger = Logger("wallet.log", "controller.log")
        self.disk_mapping_file = disk_mapping_file
        self.__load_disks_mapping()

    def __load_disks_mapping(self):
        mapping_file = open(self.disk_mapping_file, "r")
        mapping_json = json.loads(mapping_file.read())
        mapping_file.close()
        log = "\nReloading disks mapping\n"

        for disk_json in mapping_json:
            disk_id = disk_json["ID_SERIAL"]
            disk_params = {"name": disk_json["name"], "mount_directory": disk_json["mount_directory"], "partition_uuid": disk_json["partition_uuid"]}
            self.disks_mapping[disk_id] = disk_params
            log += "[" + disk_json["ID_SERIAL"] + "]: " + str(disk_params) + "\n"

        self.logger.controller_log(log)

    def __unmount_disk(self):
        pass

    def __mount_disk(self):
        pass

    def run(self):
        while(self.enabled):
            time.sleep(0.5)
        self.logger.controller_log("Controller stopped")

    def handle_signal(self, _signal):
        self.logger.controller_log("Received signal: " + str(_signal))

        if _signal == signal.SIGINT:
            self.enabled = False
            self.logger.controller_log("Turning controller off ...")

        elif _signal == signal.SIGRTMIN:
            self.__load_disks_mapping()

        elif _signal == signal.SIGUSR1:
            self.__load_disks_mapping()

        elif _signal == signal.SIGUSR2:
            self.__load_disks_mapping()

        else:
            self.logger.controller_log("Signal: " + str(_signal) + " received but unhandled")

# --- END OF CONTROLLER

controller = Controller("disks.json")

def signal_handler(_signal, frame):
    controller.handle_signal(_signal)
    return


signal.signal(signal.SIGUSR1, signal_handler)
signal.signal(signal.SIGUSR2, signal_handler)
signal.signal(signal.SIGRTMIN, signal_handler)
signal.signal(signal.SIGINT, signal_handler) 


def main():
    controller.run()

if __name__ == "__main__":
    main()

        
        


