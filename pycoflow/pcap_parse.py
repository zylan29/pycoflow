import pcap
import dpkt
import socket

from packet import Packet


def parse_pcap(pcap_file):
    pcap_obj = pcap.pcap(pcap_file)
    for ts, pkt in pcap_obj:
        eth = dpkt.ethernet.Ethernet(pkt)
        tos = eth.ip.tos
        src_ip = socket.inet_ntoa(eth.ip.src)
        src_port = eth.ip.tcp.sport
        dst_ip = socket.inet_ntoa(eth.ip.dst)
        dst_port = eth.ip.tcp.dport
        size = eth.ip.len
        yield Packet(tos, ts, src_ip, src_port, dst_ip, dst_port, size)