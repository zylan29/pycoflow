from coflows import Coflows
from packet import Packet
from flow import LogicalFlow
from utils.ip import *
from utils.path import *
from filter import packet_filter
from pcap_parse import parse_pcap
from pcap_parse import analysis_retransmit_dir


class CoflowParse(object):
    """
    parse coflows
    """
    def __init__(self):
        self.coflows = Coflows()

    def print_coflows(self):
        for coflow_id in self.coflows.coflow_ids[1:]:
            print self.coflows.coflows[coflow_id]

    def parse_dir(self, flow_files_dir):
        for flow_file in list_files(flow_files_dir, 'txt'):
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
                if not packet_filter(packet):
                    self.coflows.add_packet(packet)

    def parse_retransmit(self, pcap_dir):
        retransmit_dir = analysis_retransmit_dir(pcap_dir)
        for retransmit_file in list_files(retransmit_dir):
            packets = parse_pcap(retransmit_file)
            for packet in packets:
                if not packet_filter(packet):
                    self.coflows.add_retransmit_packet(packet)

    def parse_pcap_dir(self, pcap_dir):
        for flow_file in list_files(pcap_dir):
            self.parse_pcap_file(flow_file)

    def parse_pcap_file(self, pcap_file):
        host_name = filename_to_hostname(pcap_file)
        host_ip = host2ip(host_name)
        packets = parse_pcap(pcap_file, 'src host %s' % host_ip)
        for packet in packets:
            if not packet_filter(packet):
                self.coflows.add_packet(packet)

    def parse_log_dir(self, log_files_dir):
        for log_file in list_files(log_files_dir):
            self.parse_log_file(log_file)

    def parse_log_file(self, log_file):
        host_name = filename_to_hostname(log_file)
        host_ip = host2ip(host_name)
        with open(log_file) as f:
            log_lines = f.readlines()
            for log_line in log_lines:
                logical_flow = LogicalFlow.from_log_line(host_ip, log_line)
                if logical_flow.dst_ip == host_ip:
                    continue
                self.coflows.add_logical_flow(logical_flow)

    def start_time_offsets(self):
        for coflow_id in self.coflows.coflow_ids[1:]:
            earliest_time = 0
            for flow in coflow_parse.coflows.coflows[coflow_id].realistic_flows.values():
                if earliest_time == 0:
                    earliest_time = flow.start_time
                else:
                    if flow.start_time < earliest_time:
                        earliest_time = flow.start_time
            for flow in coflow_parse.coflows.coflows[coflow_id].realistic_flows.values():
                if flow.start_time != earliest_time:
                    print (flow.start_time - earliest_time).total_seconds()

if __name__ == '__main__':
    parse_hosts("/etc/hosts")
    coflow_parse = CoflowParse()
    coflow_parse.parse_log_dir("/home/zyang/telogs/8-logs")
    coflow_parse.parse_pcap_dir("/home/zyang/telogs/8-pcap/")
    coflow_parse.parse_retransmit("/home/zyang/telogs/8-pcap/")
    coflow_parse.print_coflows()