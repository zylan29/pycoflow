import pcap
import dpkt
import socket

from packet import Packet
from utils.time import convert_epoch_time


def parse_pcap(host, pcap_file):
    pcap_obj = pcap.pcap(pcap_file)
    pcap_obj.setfilter('src host %s' % host)
    for ts, pkt in pcap_obj:
        eth = dpkt.ethernet.Ethernet(pkt)
        tos = eth.ip.tos
        shuffle_id = tos / 4 - 1
        src_ip = socket.inet_ntoa(eth.ip.src)
        src_port = eth.ip.tcp.sport
        dst_ip = socket.inet_ntoa(eth.ip.dst)
        dst_port = eth.ip.tcp.dport
        size = eth.ip.len
        yield Packet(shuffle_id, convert_epoch_time(ts), src_ip, src_port, dst_ip, dst_port, size)