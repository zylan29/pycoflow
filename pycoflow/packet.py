__author__ = 'anzigly'
from utils.time import TimeUtils


class Packet(object):
    """
    a packet abstraction
    """
    def __init__(self, packet_time, src_ip, src_port, dst_ip, dst_port, packet_size):
        self.packet_time = packet_time
        self.src_ip = src_ip
        self.src_port = src_port
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.packet_size = packet_size

    @staticmethod
    def from_line_str(flow_line):
        """
        get packet object from a line in captured file.
        :param flow_line: a line in captured file.
        :return: a packet object
        """
        try:
            [packet_time, src_ip, src_port, dst_ip, dst_port, packet_size] = flow_line.split("\t")
            if len(packet_time) == 16 and packet_time.endswith("000"):
                packet_time = packet_time[:-3]
            packet_time = TimeUtils.time_convert(packet_time)
            packet_size = int(packet_size)
        except ValueError:
            return None
        else:
            return Packet(packet_time, src_ip, src_port, dst_ip, dst_port, packet_size)