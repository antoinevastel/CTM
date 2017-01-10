import json
import pyspeedtest

from scheduler.task import Task
from utils.sysUtils import runCommand


class TcpSpeedTestTask(Task):
    TAG = "TCP speed test"

    def __init__(self, json_definition):
        super(TcpSpeedTestTask, self).__init__(json_definition)

    def run(self):
        st = pyspeedtest.SpeedTest()
        key_value_to_export = dict()
        key_value_to_export[Task.TAG_KEY] = TcpSpeedTestTask.TAG
        key_value_to_export["download_speed"] = st.download() / 1000000
        key_value_to_export["upload_speed"] = st.upload() / 1000000
        key_value_to_export["latency"] = st.ping()

        return json.dumps(key_value_to_export)
