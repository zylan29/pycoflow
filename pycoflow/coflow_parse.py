from application import Application
from flow import LogicalFlow
from utils.ip import *
from utils.path import *
from utils.time import TimeUtils
from filter import packet_filter
from pcap_parse import parse_pcap
from pcap_parse import analysis_retransmit_dir


def app_id_to_int(app_id):
    return int(app_id.split('-')[-1])


def packet_coflow_str(packet, coflow_id, app_id):
    packet_str = [TimeUtils.time_offset(packet.packet_time), ip2int(packet.src_ip), packet.src_port,
                  ip2int(packet.dst_ip), packet.dst_port, app_id_to_int(app_id), coflow_id, packet.packet_size]
    return " ".join(map(str, packet_str))


class CoflowParse(object):
    """
    parse coflows
    """
    def __init__(self):
        self.applications = {}
        self.start_time = None

    def __str__(self):
        return '\n'.join(map(str, self.applications.values()))

    def parse_applications(self, applications_file):
        with open(applications_file, 'r') as f:
            for app in map(lambda x: x.strip(), f.readlines()):
                if app not in self.applications:
                    self.applications[app] = Application()
        print self.applications.keys()

    def parse_retransmit(self, pcap_dir):
        retransmit_dir = analysis_retransmit_dir(pcap_dir)
        for retransmit_file in list_files(retransmit_dir):
            packets = parse_pcap(retransmit_file)
            for packet in packets:
                if not packet_filter(packet):
                    self.applications.add_retransmit_packet(packet)

    def parse_pcap_dir(self, pcap_dir, packets_file='packet.txt'):
        for flow_file in list_files(pcap_dir):
            self.parse_pcap_file(flow_file, packets_file)

    def parse_pcap_file(self, pcap_file, packets_file='packet.txt'):
        """
        Output parsed packets to packets_file
        """
        host_name = filename_to_hostname(pcap_file)
        host_ip = host2ip(host_name)
        packets = parse_pcap(pcap_file, 'src host %s' % host_ip)
        f = open(packets_file, 'aw')
        for packet in packets:
            if not self.start_time:
                self.start_time = packet.packet_time
            else:
                if packet.packet_time < self.start_time:
                    self.start_time = packet.packet_time
            if not packet_filter(packet):
                found = False
                for app_id in self.applications:
                    coflow_id = self.applications[app_id].add_packet(packet)
                    if coflow_id:
                        packet_str = packet_coflow_str(packet, coflow_id, app_id)
                        f.write(packet_str + "\n")
                        found = True
                        break
                if not found:
                    print packet
        f.close()

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
                self.applications[logical_flow.app_id].add_logical_flow(logical_flow)

    def output_flows(self, flows_file):
        with open(flows_file, 'w') as f:
            for app_id in self.applications:
                for coflow_id in self.applications[app_id].coflows:
                    flow_id = app_id + '-' + coflow_id
                    for flow in self.applications[app_id].coflows[coflow_id].realistic_flows.values():
                        f.write(flow.output_flow(flow_id))
                        f.write('\n')

    def start_time_offsets(self):
        for coflow_id in self.applications.coflow_ids[1:]:
            earliest_time = 0
            for flow in coflow_parse.applications.coflows[coflow_id].realistic_flows.values():
                if earliest_time == 0:
                    earliest_time = flow.start_time
                else:
                    if flow.start_time < earliest_time:
                        earliest_time = flow.start_time
            for flow in coflow_parse.applications.coflows[coflow_id].realistic_flows.values():
                if flow.start_time != earliest_time:
                    print (flow.start_time - earliest_time).total_seconds()

if __name__ == '__main__':
    parse_hosts("/etc/hosts")
    coflow_parse = CoflowParse()
    coflow_parse.parse_applications('app.txt')
    coflow_parse.parse_log_dir("/home/zyang/telogs/5-logs")
    coflow_parse.parse_pcap_dir("/home/zyang/telogs/5-pcap")
    coflow_parse.output_flows('flows.txt')