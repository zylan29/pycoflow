import subprocess
import pcap
import dpkt
import socket
import os

from packet import Packet
from utils.time import TimeUtils
from utils.path import list_files


def parse_pcap(pcap_file, filters=''):
    pcap_obj = pcap.pcap(pcap_file)
    if filters != '':
        pcap_obj.setfilter(filters)
    for ts, pkt in pcap_obj:
        eth = dpkt.ethernet.Ethernet(pkt)
        tos = eth.ip.tos
        shuffle_id = tos / 4 - 1
        src_ip = socket.inet_ntoa(eth.ip.src)
        src_port = eth.ip.tcp.sport
        dst_ip = socket.inet_ntoa(eth.ip.dst)
        dst_port = eth.ip.tcp.dport
        size = eth.ip.len
        yield Packet(shuffle_id, TimeUtils.time_convert(ts), src_ip, src_port, dst_ip, dst_port, size)


def analysis_retransmit_dir(pcap_dir):
    retransmit_dir = pcap_dir + '/retransmit/'
    if not os.path.exists(retransmit_dir):
        os.mkdir(retransmit_dir)
    for pcap_file in list_files(pcap_dir):
        _, file_name = os.path.split(pcap_file)
        retransmit_file = retransmit_dir + file_name
        analysis_retransmission(pcap_file, retransmit_file)
    return retransmit_dir


def tshark_analysis(pcap_file, out_file, filters):
    if isinstance(filters, list):
        filters = ' '.join(filters)

    print 'Analysing packets from %s' % pcap_file
    print 'Filters are: %s' % filters

    cmd = 'tshark -r %s "%s" -w %s' % (pcap_file, filters, out_file)
    p = subprocess.Popen(cmd, shell=True, universal_newlines=True)
    ret_code = p.wait()
    print 'Analysing retransmission packets completed, \n return_code=%s, \n writen result to %s' \
          % (ret_code, out_file)
    return ret_code


def analysis_retransmission(pcap_file, retrans_pcap_file):
    return tshark_analysis(pcap_file, retrans_pcap_file, "tcp.analysis.retransmission")


def analysis_dup_ack(pcap_file, dup_ack_pcap_file):
    return tshark_analysis(pcap_file, dup_ack_pcap_file, "tcp.analysis.duplicate_ack")