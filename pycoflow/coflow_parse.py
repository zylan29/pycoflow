import os

from coflows import Coflows
from packet import Packet
from flow import LogicalFlow


class CoflowParse(object):
    """
    parse coflows
    """
    def __init__(self):
        self.coflows = Coflows()

    def parse_dir(self, flow_files_dir):
        root, dirs, flow_files = os.walk(flow_files_dir).next()
        if not root.endswith("/"):
            root += "/"
        for flow_file in flow_files:
            self.parse_file(flow_file)

    def parse_file(self, flow_file):
        with open(flow_file) as f:
            flow_lines = f.readlines()
            for flow_line in flow_lines:
                packet = Packet.from_line_str(flow_line)
                self.coflows.add_packet(packet)

    def parse_log_file(self, log_file):
        with open(log_file) as f:
            log_lines = f.readlines()
            for log_line in log_lines:
                logical_flow = LogicalFlow.from_log_line(log_line)
                self.coflows.add_logical_flow(logical_flow)

if __name__ == '__main__':
    coflow_parse = CoflowParse()
    coflow_parse.parse_log_file("a log file here")
    coflow_parse.parse_dir("a dir contains captured packet files")
    print(coflow_parse.coflows)