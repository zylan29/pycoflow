import os

from coflows import Coflows
from packet import Packet
from flow import LogicalFlow
from utils.ip import IP


class CoflowParse(object):
    """
    parse coflows
    """
    def __init__(self, hosts):
        self.coflows = Coflows()
        self.hosts = hosts

    def parse_dir(self, flow_files_dir):
        root, dirs, flow_files = os.walk(flow_files_dir).next()
        # if not root.endswith("/"):
        #     root += "/"
        if not root.endswith("\\"):
            root += "\\"
        for flow_file in flow_files:
            flow_file = root+flow_file
            self.parse_file(flow_file)

    def parse_file(self, flow_file):
        i = 1
        with open(flow_file) as f:
            flow_lines = f.readlines()
            for flow_line in flow_lines:
                if i == 1:
                    i += 1
                    continue
                packet = Packet.from_line_str(flow_line)
                self.coflows.add_packet(packet)

    def parse_log_file(self, log_file):
        with open(log_file) as f:
            log_lines = f.readlines()
            for log_line in log_lines:
                logical_flow = LogicalFlow.from_log_line(log_line, hosts)
                self.coflows.add_logical_flow(logical_flow)


if __name__ == '__main__':
    ip = IP()
    hosts = ip.parse_hosts("C:\Users\Administrator\Downloads\hosts")
    coflow_parse = CoflowParse(hosts)
    # coflow_parse.parse_log_file("C:\Users\Administrator\Downloads\logWordCount")
    # coflow_parse.parse_dir("C:\Users\Administrator\Downloads\\tsWordCount")
    coflow_parse.parse_log_file("C:\Users\Administrator\Downloads\logPageRank")
    coflow_parse.parse_dir("C:\Users\Administrator\Downloads\\tsPageRank")
    print(coflow_parse.coflows)