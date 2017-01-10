import json

from scheduler.task import Task
from utils.sysUtils import runCommand

class TracerouteHopOutput():
    def __init__(self, ip_addr, rtts):
        self.ip_addr = ip_addr
        self.rtts = rtts

    def to_sjson(self):
        key_value_to_export = dict()
        key_value_to_export["destination"] = self.ip_addr
        key_value_to_export["rtts"] = self.rtts
        return json.dumps(key_value_to_export)

class TracerouteOutput():
    UNKNOWN = "*"
    TAG = "Traceroute"

    def __init__(self, destination, raw_output):
        self.destination = destination
        self.hops = list()
        self.raw_output = raw_output

        self.add_hops()

    def add_hops(self):
        for l in self.raw_output:
            l = l.split("  ")
            #we extract ip address
            if len(l) > 1:
                start_parenthesis = l[1].find("(")
                end_parenthesis = l[1].find(")")
                if start_parenthesis > -1 and end_parenthesis > -1:
                    try:
                        ipAddr = l[1][start_parenthesis + 1:end_parenthesis]
                    except:
                        ipAddr = TracerouteOutput.UNKNOWN

                    try:
                        rtt1 = float(l[2].replace(" ms", ""))
                    except:
                        rtt1 = TracerouteOutput.UNKNOWN

                    try:
                        rtt2 = float(l[3].replace(" ms", ""))
                    except:
                        rtt2 = TracerouteOutput.UNKNOWN

                    try:
                        rtt3 = float(l[4].replace(" ms", ""))
                    except:
                        rtt3 = TracerouteOutput.UNKNOWN

                    self.add_hop(ipAddr, [rtt1, rtt2, rtt3])
                else:
                    self.add_hop(TracerouteOutput.UNKNOWN, [TracerouteOutput.UNKNOWN, TracerouteOutput.UNKNOWN, TracerouteOutput.UNKNOWN])

    def add_hop(self, ip_addr, rtts):
        self.hops.append(TracerouteHopOutput(ip_addr, rtts))

    def to_sjson(self):
        key_val_to_export = dict()
        key_val_to_export[Task.TAG_KEY] = TracerouteOutput.TAG
        key_val_to_export["destination"] = self.destination
        key_val_to_export["hops"] = []
        for hop in self.hops:
            key_val_to_export["hops"].append(hop.to_sjson())

        return json.dumps(key_val_to_export)

class TracerouteTask(Task):
    TARGET_URL = "target_url"

    def __init__(self, json_definition):
        super(TracerouteTask, self).__init__(json_definition)

        self.target_url = json_definition[TracerouteTask.TARGET_URL]


    def run(self):
        traceroute_raw_output, traceroute_error = runCommand("traceroute "+self.target_url)
        traceroute_raw_output = traceroute_raw_output.split("\n")[1:]
        traceroute_output = TracerouteOutput(self.target_url, traceroute_raw_output)
        return traceroute_output.to_sjson()