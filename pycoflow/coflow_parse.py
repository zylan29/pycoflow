__author__ = 'anzigly'
import os

from coflows import Coflows
from packet import Packet


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