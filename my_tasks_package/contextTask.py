import json
import netifaces

from scheduler.task import Task
from utils.sysUtils import runCommand


class ContextTask(Task):

    TAG = "context"

    def __init__(self, json_definition):
        super(ContextTask, self).__init__(json_definition)

    def run(self):
        def_gw_device = netifaces.gateways()['default'][netifaces.AF_INET][1]
        mac_addr = netifaces.ifaddresses(def_gw_device)[netifaces.AF_LINK][0]["addr"]
        # We test if the default interface is wifi
        is_wifi = True
        iw_output, iw_error = runCommand("iwconfig " + def_gw_device)
        if "no wireless extensions." in iw_error:
            is_wifi = False

        key_value_to_export = dict()
        key_value_to_export[Task.TAG_KEY] = ContextTask.TAG
        key_value_to_export["network_interface"] = def_gw_device
        key_value_to_export["mac_address"] = mac_addr
        key_value_to_export["wifi"] = is_wifi

        return json.dumps(key_value_to_export)
