from datetime import datetime
from gpiozero import LED
from sh import findmnt, ls, mount, umount, pgrep
from urllib.request import urlopen
from email.mime.text import MIMEText

import glob
import json
import os
import requests
import signal
import smtplib
import subprocess
import time

# GLOBAL VARIABLES SECTION
FARMER_VALID_PLOTS_COUNT = 862

LED_CTRL = LED(17)

BREAK_BETWEEN_JOBS_IN_SECONDS = 180
STARTUP_HOLD_TIME_IN_SECONDS = 30
WALLET_DATA_STORE_INTERVAL = 86400 # once per day
EMAIL_SEND_INTERVAL = 86400 # once per day

WALLET_LOG_PATH="/home/raspberry/wallet.log"

CONTROLLER_LOG_PATH="/home/raspberry/controller.log"
DISKS_JSON_FILE_PATH = "/home/raspberry/controller/disks.json"

RPC_REQUEST_HEADERS = {'Content-Type': 'application/json'}
BLOCKCHAIN_STATE_URL = "https://localhost:8555/get_blockchain_state"
FARMER_STATE_URL = "https://localhost:8559/get_harvesters_summary"
WALLET_BALANCE_URL = "https://localhost:9256/get_wallet_balance"
CHIA_ROOT_DIR = os.getenv("CHIA_ROOT")  # shouldn't have '/' in the end.
FULL_NODE_CERT = (CHIA_ROOT_DIR + '/config/ssl/full_node/private_full_node.crt', CHIA_ROOT_DIR + '/config/ssl/full_node/private_full_node.key')

CHIA_NODE_PROCESS_NAME = "chia_full_node"
PATH_TO_RUN_CHIA_SCRIPT = "/home/raspberry/controller/chia_full_node.sh"

MANDATORY_DISKS_MOUNTED = False
CONTROLLER_ENABLED = True
ALL_DISKS_CONNECTED = True
NETWORK_WORKS = True
NODE_SYNCED = True
CHIA_NODE_ENABLED = True
FARMER_SYNCED = True
FARMER_PLOTS_NUMBER_GOOD = True


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

    def __init__(self):
        self.wallet_log_path = WALLET_LOG_PATH
        self.controller_log_path = CONTROLLER_LOG_PATH
        self.wallet_logs_buffer = ""
        self.ctrl_logs_buffer = ""
        self.wallet_log("Controller started work")
        self.controller_log("Controller started work")

    def __log(self, file, log):
        log_file = open(file, "a")
        log_file.write(log)
        log_file.close()

    def wallet_log(self, log):
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        log = '[' + dt_string + "]: " + log + '\n' 
        self.__log(self.wallet_log_path, log)
        self.wallet_logs_buffer += log

    def controller_log(self, log):
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        log = '[' + dt_string + "]: " + log + '\n' 
        self.__log(self.controller_log_path, log)
        self.ctrl_logs_buffer += log + '\n'

    def get_logs_from_buffer(self):
        logs = "Wallet logs:\n" + self.wallet_logs_buffer + "\n"
        logs += "Controller logs:\n" + self.ctrl_logs_buffer
        return logs

    def clear_logs_buffers(self):
        self.wallet_logs_buffer = ""
        self.ctrl_logs_buffer = ""

# --- END OF LOGGER

class Controller:

    # {"disk_uuid": {"mount_point", "is_mounted", "is_mandatory"}}
    disks_mapping = {}

    def __init__(self, disks_mapping_file):
        self.previous_confirmed_amount = 0;
        self.logger = Logger()
        self.disks_mapping_file = disks_mapping_file
        self.mandatory_disks_amount = 0

    def __load_disks_mapping(self):
        loaded_mapping_file = open(self.disks_mapping_file, "r")
        loaded_mapping_json = json.loads(loaded_mapping_file.read())
        loaded_mapping_file.close()
        mapping_updated = False
        loaded_disk_ids = []

        for loaded_disk_json in loaded_mapping_json:
            loaded_disk_id = loaded_disk_json["disk_uuid"]
            loaded_disk_mount_point = loaded_disk_json["mount_point"]
            loaded_disk_is_mandatory = loaded_disk_json["is_mandatory"]

            loaded_disk_ids.append(loaded_disk_id)

            if loaded_disk_id not in self.disks_mapping.keys():
                loaded_disk_params = { "mount_point": loaded_disk_mount_point, "is_mounted": False, "is_mandatory": loaded_disk_is_mandatory }
                self.disks_mapping[loaded_disk_id] = loaded_disk_params

                if loaded_disk_is_mandatory:
                    self.mandatory_disks_amount += 1

                mapping_updated = True
                self.__umount_disk(loaded_disk_id, loaded_disk_mount_point)

            else:
                current_disk_params = self.disks_mapping[loaded_disk_id]

                if current_disk_params["mount_point"] != loaded_disk_mount_point:
                    self.disks_mapping[disk_id]["mount_point"] = loaded_disk_mount_point
                    self.__umount_disk(disk_id, current_disk_params["mount_point"])
                    self.__umount_disk(disk_id, loaded_disk_mount_point)
                    mapping_updated = True
                if current_disk_params["is_mandatory"] != loaded_disk_is_mandatory:
                    self.disks_mapping[disk_id]["is_mandatory"] = loaded_disk_is_mandatory

                    if loaded_disk_is_mandatory:
                        self.mandatory_disks_amount += 1
                    else:
                        self.mandatory_disks_amount -= 1

                    mapping_updated = True


        disk_ids_to_remove = []
        for disk_id, disk_params in self.disks_mapping.items():
            if disk_id not in loaded_disk_ids:
                self.__umount_disk(disk_id, disk_params["mount_point"])
                disk_ids_to_remove.append(disk_id)

                if disk_params["is_mandatory"]:
                    self.mandatory_disks_amount -= 1

        for disk_id in disk_ids_to_remove:
            self.disks_mapping.pop(disk_id)
            mapping_updated = True

        if mapping_updated:
            self.logger.controller_log("Reloaded disks mapping:\n" + str(self.disks_mapping) + "\n")
            self.logger.controller_log("Mandatory disks amount:" + str(self.mandatory_disks_amount) + "\n")

    # - end of load disks mapping impl

    def __umount_disk(self, disk_id, mount_point):
        self.disks_mapping[disk_id]["is_mounted"] = False

        if not os.path.isdir(mount_point):
            self.logger.controller_log("[Error] Unmounting " + mount_point + " failed. Mount point doesn't exists")
            return

        mounted_filesystems = ""

        try:
            mounted_filesystems = str(findmnt(mount_point, "-J"))
        except:
            self.logger.controller_log("[Error] No filesystems mounted on " + mount_point)
            return

        try:
            mounted_filesystems_json = json.loads(mounted_filesystems)
            filesystems_mounted_count = len(mounted_filesystems_json["filesystems"])
            self.logger.controller_log("Unmounting " + mount_point + ". Found " + str(filesystems_mounted_count) + " filesystems mounted there.")
            for i in range(0, filesystems_mounted_count):
              umount(mount_point)
        except Exception as e:
            self.logger.controller_log("[Error] Unmounting " + mount_point + " failed. Error: " + str(e))

    # - end of umount disk impl

    def __mount_disk(self, disk_id, mount_point):
        if not os.path.isdir(mount_point):
            self.logger.controller_log("[Error] Mounting " + disk_id + " to " + mount_point + " failed. Mount point doesn't exists")
            return

        try:
            ls("/dev/disk/by-uuid/" + disk_id)
        except:
            self.logger.controller_log("[Error] Mounting " + disk_id + " to " + mount_point + " failed. Disk is not connected!")
            return

        try:
            mount("UUID="+ disk_id, mount_point)
            self.disks_mapping[disk_id]["is_mounted"] = True
            plots_count = len(glob.glob1(mount_point, "*.plot"))
            self.logger.controller_log("Mounted " + disk_id + ". Mount point: " + mount_point + ". Plots count: " + str(plots_count))
            time.sleep(3)
        except Exception as e:
            self.logger.controller_log("[Error] Mounting " + disk_id + " to " + mount_point + " failed. Error: " + str(e))

    # - end of mount disk impl

    def __check_mount_points(self):
        global ALL_DISKS_CONNECTED
        global MANDATORY_DISKS_MOUNTED
        connected_disks_count = 0
        connected_mandatory_disks_count = 0

        for disk_id, disk_params in self.disks_mapping.items():
            mount_point = disk_params["mount_point"]
            disk_mounted = disk_params["is_mounted"]
            disk_is_mandatory = disk_params["is_mandatory"]

            if disk_mounted:
                files_cnt = 0
                try:
                    files_cnt = len(os.listdir(mount_point))
                    if files_cnt == 0:
                        self.__umount_disk(disk_id, mount_point)
                        disk_mounted = False
                    else:
                        connected_disks_count += 1

                except Exception as e:
                    self.logger.controller_log("[Error] Cannot check the content of mount point '" + mount_point + "'. Unmouting disk. Error: " + str(e))
                    self.__umount_disk(disk_id, mount_point)
                    disk_mounted = False

            if not disk_mounted:
                files_cnt = 0
                try:
                    files_cnt = len(os.listdir(mount_point))
                except Exception as e:
                    self.logger.controller_log("[Error] Cannot check the content of mount point '" + mount_point + "'. Error: " + str(e))
                else:
                    if files_cnt != 0:
                        self.logger.controller_log("[Error] Cannot mount disk " + disk_id + " to " + mount_point + " because mount point contains files.")
                    else:
                        self.__mount_disk(disk_id, mount_point)
                        try:
                            files_cnt = len(os.listdir(mount_point))
                        except Exception as e:
                            self.logger.controller_log("[Error] Cannot check the content of mount point '" + mount_point + "'. Error: " + str(e))
                        else:
                            if files_cnt != 0:
                                connected_disks_count += 1
                                disk_mounted = True

            if disk_is_mandatory and disk_mounted:
                connected_mandatory_disks_count += 1
            if disk_is_mandatory and not disk_mounted:
                self.logger.controller_log("[Error] Mandatory disk cannot be mounted to: " + mount_point)

        if connected_disks_count == len(self.disks_mapping):
            if not ALL_DISKS_CONNECTED:
                self.logger.controller_log("All disks are connected and mounted again!")

            ALL_DISKS_CONNECTED = True
        else:
            ALL_DISKS_CONNECTED = False

        if connected_mandatory_disks_count == self.mandatory_disks_amount:
            if not MANDATORY_DISKS_MOUNTED:
                self.logger.controller_log("All mandatory disks are connected and mounted again!")

            MANDATORY_DISKS_MOUNTED = True
        else:
            MANDATORY_DISKS_MOUNTED = False

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
                self.logger.controller_log("[Error] Network connection has been lost.")

    # - end of check network impl

    def __check_blockchain_sync(self):
        try:
            response = json.loads(requests.post(BLOCKCHAIN_STATE_URL, data='{}', headers=RPC_REQUEST_HEADERS, cert=FULL_NODE_CERT, verify=False).text)
            is_synced = response["blockchain_state"]["sync"]["synced"]
            global NODE_SYNCED

            if not is_synced and NODE_SYNCED:
                self.logger.controller_log("[Error] Node is not synchronized with blockchain.")
                NODE_SYNCED = False
            elif is_synced and not NODE_SYNCED:
                NODE_SYNCED = True
                self.logger.controller_log("Node is synchronized with blockchain.")

        except requests.ConnectionError as error:
            NODE_SYNCED = False
            self.logger.controller_log("[Error] Cannot send request to full node to check if blockchain is synced. Connection error: " + str(error))
        except requests.HTTPError as error:
            NODE_SYNCED = False
            self.logger.controller_log("[Error] Cannot send request to full node to check if blockchain is synced. Http error: " + str(error))
        except Exception as e:
            NODE_SYNCED = False
            self.logger.controller_log("[Error] Cannot send request to full node to check if blockchain is synced. " + str(e))

    # - end of check blockchain sync impl

    def __chia_node_action(self, turn_on):
        global CHIA_NODE_ENABLED
        try:
            if turn_on:
                output = subprocess.check_output(PATH_TO_RUN_CHIA_SCRIPT + " start", shell=True)
                self.logger.controller_log(CHIA_NODE_PROCESS_NAME + " restarted. Output:\n" + str(output))
                time.sleep(30) #let's wait a little to let process be ready to work.
            else:
                output = subprocess.check_output(PATH_TO_RUN_CHIA_SCRIPT, shell=True)
                self.logger.controller_log(CHIA_NODE_PROCESS_NAME + " stopped. Output:\n" + str(output))
                CHIA_NODE_ENABLED = False
        except subprocess.CalledProcessError as error:
            self.logger.controller_log("[Error] Failed to run script: " + PATH_TO_RUN_CHIA_SCRIPT + ". Error: " + str(error))
            CHIA_NODE_ENABLED = False


    def __is_process_alive(self):
        global CHIA_NODE_ENABLED
        try:
            pgrep(CHIA_NODE_PROCESS_NAME)
            if not CHIA_NODE_ENABLED:
                CHIA_NODE_ENABLED = True
                self.logger.controller_log(CHIA_NODE_PROCESS_NAME + " process works again.")
        except:
            CHIA_NODE_ENABLED = False
            self.logger.controller_log("[Error] " + CHIA_NODE_PROCESS_NAME + " stopped working. Restarting ...")
            self.__chia_node_action(True)

    # - end of is_process_alive

    def __notify_if_problem(self):
        global NETWORK_WORKS
        global NODE_SYNCED
        global CHIA_NODE_ENABLED
        global ALL_DISKS_CONNECTED
        global LED_CTRL
        global FARMER_PLOTS_NUMBER_GOOD
        global FARMER_SYNCED
        global MANDATORY_DISKS_MOUNTED

        if not NETWORK_WORKS or not NODE_SYNCED or not CHIA_NODE_ENABLED or not ALL_DISKS_CONNECTED or not FARMER_SYNCED or not FARMER_PLOTS_NUMBER_GOOD or not MANDATORY_DISKS_MOUNTED:
            LED_CTRL.on()
        else:
            LED_CTRL.off()

    # - end of notify if problem

    def __check_farmer(self):
        try:
            response = json.loads(requests.post(FARMER_STATE_URL, data='{}', headers=RPC_REQUEST_HEADERS, cert=FULL_NODE_CERT, verify=False).text)
            harvester = response["harvesters"][0]
            global FARMER_SYNCED
            global FARMER_PLOTS_NUMBER_GOOD

            if FARMER_SYNCED and harvester["syncing"] is not None:
                self.logger.controller_log("[Error] Farmer is not synchronized with blockchain.")
                FARMER_SYNCED = False
            elif not FARMER_SYNCED and harvester["syncing"] is None:
                self.logger.controller_log("Farmer is synchronized with blockchain.")
                FARMER_SYNCED = True

            if FARMER_PLOTS_NUMBER_GOOD and harvester["plots"] != FARMER_VALID_PLOTS_COUNT:
                self.logger.controller_log("[Error] Farmer's plots count is not good. Plots count: " + str(harvester["plots"]) + ", expected: " + str(FARMER_VALID_PLOTS_COUNT))
                FARMER_PLOTS_NUMBER_GOOD = False
            elif not FARMER_PLOTS_NUMBER_GOOD and harvester["plots"] == FARMER_VALID_PLOTS_COUNT:
                self.logger.controller_log("Farmer's plots count is good again.")
                FARMER_PLOTS_NUMBER_GOOD = True

        except requests.ConnectionError as error:
            FARMER_SYNCED = False
            self.logger.controller_log("[Error] Cannot send request to full node to check if farmer is synced. Connection error: " + str(error))
        except requests.HTTPError as error:
            FARMER_SYNCED = False
            self.logger.controller_log("[Error] Cannot send request to full node to check if farmer is synced. Http error: " + str(error))
        except Exception as e:
            FARMER_SYNCED = False
            self.logger.controller_log("[Error] Cannot send request to full node to check if farmer is synced. " + str(e))

    # - check farmer plots count

    def __store_wallet_data(self):
        try:
            response = json.loads(requests.post(WALLET_BALANCE_URL, data='{"wallet_id":1}', headers=RPC_REQUEST_HEADERS, cert=FULL_NODE_CERT, verify=False).text)
            current_confirmed_amount = response["wallet_balance"]["confirmed_wallet_balance"]
            diff_in_mojos = current_confirmed_amount - self.previous_confirmed_amount
            MOJO_TO_CHIA_DIVIDER = 1000000000000
            diff_in_chia = diff_in_mojos / MOJO_TO_CHIA_DIVIDER
            self.previous_confirmed_amount = current_confirmed_amount
            self.logger.wallet_log("Wallet 1 - confirmed: " + str(current_confirmed_amount) + " (diff: " + str(round(diff_in_chia, 4)) + " xch, " + str(diff_in_mojos) + " mojos), spendable: " + str(response["wallet_balance"]["spendable_balance"]))
            return True

        except Exception as e:
            self.logger.wallet_log("[Error] Unable to get wallet's balance. " + str(e))
            return False

    # - store wallet data

    def __send_email(self):
        try:
            f = open("/home/raspberry/controller/emailpass.txt", "r")
            password = f.read()
            password = password[:len(password) - 1]
            f.close()
            sender = "koparaumalca@int.pl"
            recipients = ["xaidianx@gmail.com", "doew123@gmail.com"]
            body = "Logi\n"
            body += self.logger.get_logs_from_buffer()
            msg = MIMEText(body)
            msg['Subject'] = "Raport dobowy z kopary"
            msg['From'] = sender
            msg['To'] = ', '.join(recipients)
            smtp_server = smtplib.SMTP_SSL('poczta.int.pl', 465)
            smtp_server.login(sender, password)
            smtp_server.sendmail(sender, recipients, msg.as_string())
            smtp_server.quit()
            self.logger.controller_log("Email with report has been sent.")
            self.logger.clear_logs_buffers()
            return True
        except Exception as e:
            self.logger.controller_log("[Error] Unable to send email: " + str(e))
            return False

    # - send email

    def run(self):
        global CONTROLLER_ENABLED
        global BREAK_BETWEEN_JOBS_IN_SECONDS
        global WALLET_DATA_STORE_INTERVAL
        global NODE_SYNCED
        global CHIA_NODE_ENABLED
        global MANDATORY_DISKS_MOUNTED
        global EMAIL_SEND_INTERVAL

        time.sleep(STARTUP_HOLD_TIME_IN_SECONDS)
        self.logger.controller_log("Startup hold period passed.")

        last_time = 0
        wallet_last_time_check = 0
        email_last_time_send = 0
        while CONTROLLER_ENABLED:
            now = time.time()
            if now - last_time > BREAK_BETWEEN_JOBS_IN_SECONDS:
                self.__load_disks_mapping()
                self.__check_mount_points()
                self.__check_network()

                if MANDATORY_DISKS_MOUNTED:
                    self.__is_process_alive()
                elif CHIA_NODE_ENABLED:
                    self.__chia_node_action(False)

                if CHIA_NODE_ENABLED:
                    self.__check_blockchain_sync()
                    self.__check_farmer()

                if NODE_SYNCED and now - wallet_last_time_check > WALLET_DATA_STORE_INTERVAL:
                  if self.__store_wallet_data():
                      wallet_last_time_check = now

                if NETWORK_WORKS and now - email_last_time_send > EMAIL_SEND_INTERVAL:
                  if self.__send_email():
                    email_last_time_send = now

                last_time = time.time()
                self.__notify_if_problem()

            time.sleep(10)

        self.logger.controller_log("Stopping " + CHIA_NODE_PROCESS_NAME + " ...")
        self.__chia_node_action(False)
        self.logger.controller_log("Controller is stopped with sigint or sigterm")
        self.logger.wallet_log("Controller is stopped with sigint or sigterm")

# --- END OF CONTROLLER

if __name__ == '__main__':
    signal.signal(signal.SIGINT, handleSigInt)
    signal.signal(signal.SIGTERM, handleSigTerm)
    controller = Controller(DISKS_JSON_FILE_PATH)
    controller.run()
