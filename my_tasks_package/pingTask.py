import json

from scheduler.task import Task
from utils.sysUtils import runCommand


class PingOutput:
    TAG = "ping"

    def __init__(self, destination, nb_packets, nb_received, pct_loss, total_time, nb_hops, rtt_min, rtt_avg, rtt_max,
                 rtt_mdev):
        self.destination = destination
        self.nb_packets = nb_packets
        self.nb_received = nb_received
        self.pct_loss = pct_loss
        self.total_time = total_time
        self.nb_hops = nb_hops

        self.rtt_min = rtt_min
        self.rtt_avg = rtt_avg
        self.rtt_max = rtt_max
        self.rtt_mdev = rtt_mdev

    def to_sjson(self):
        attributes = [i for i in self.__dict__.keys() if i[:1] != '_']
        key_val_to_export = dict()
        for att in attributes:
            key_val_to_export[att] = (self.__dict__[att])

        key_val_to_export[Task.TAG_KEY] = PingOutput.TAG
        return json.dumps(key_val_to_export)


class PingTask(Task):

    TARGET_URL = "target_url"
    NB_PACKETS = "nb_packets"

    def __init__(self, json_definition):
        super(PingTask, self).__init__(json_definition)

        self.target_url = json_definition[PingTask.TARGET_URL]

        try:
            self.nb_packets = json_definition[PingTask.NB_PACKETS]
        except KeyError:
            self.nb_packets = 5

    def raw_ping_output_to_obj(self, raw_output, hop="*"):
        ping_out = raw_output.split("\n")

        received_loss_line = ping_out[len(ping_out) - 3].split(",")
        try:
            rtt_line = ping_out[len(ping_out) - 2].split("=")[1].split("/")  # may be empty if no response
            rtt_min = float(rtt_line[0].replace(" ", ""))
            rtt_avg = float(rtt_line[1])
            rtt_max = float(rtt_line[2])
            rtt_mdev = float(rtt_line[3].replace(" ms", ""))
        except:
            rtt_min = "*"
            rtt_avg = "*"
            rtt_max = "*"
            rtt_mdev = "*"

        nb_received = int(received_loss_line[1].split(" ")[1])
        pct_loss = float(received_loss_line[2].split(" ")[1].replace("%", ""))
        total_time = float(received_loss_line[3].split(" ")[2].replace("ms", ""))

        return PingOutput(self.target_url, self.nb_packets, nb_received, pct_loss, total_time, hop, rtt_min, rtt_avg,
                          rtt_max, rtt_mdev)

    def run(self):
        raw_ping_out, ping_err = runCommand("ping -c " + str(self.nb_packets) + " " + self.target_url)
        ping_output = self.raw_ping_output_to_obj(raw_ping_out, hop="*")
        return ping_output.to_sjson()
