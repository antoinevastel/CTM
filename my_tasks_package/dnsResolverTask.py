import time

import json
import dns.resolver

from scheduler.task import Task


class DnsResolverTask(Task):
    TAG = "DNS resolver"
    TARGET_URL = "target_url"
    RESOLUTION_TIME = "resolution_time"

    def __init__(self, json_definition):
        super(DnsResolverTask, self).__init__(json_definition)
        self.target_url = json_definition[DnsResolverTask.TARGET_URL]

    def run(self):
        start_time = time.time()
        my_resolver = dns.resolver.Resolver()
        my_resolver.query(self.target_url, "A")
        resolution_time = time.time() - start_time

        key_value_to_export = dict()
        key_value_to_export[Task.TAG_KEY] = DnsResolverTask.TAG
        key_value_to_export[DnsResolverTask.TARGET_URL] = self.target_url
        key_value_to_export[DnsResolverTask.RESOLUTION_TIME] = resolution_time

        return json.dumps(key_value_to_export)
