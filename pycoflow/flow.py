__author__ = 'anzigly'

from utils.time import TimeUtils
from packet import Packet


class Flow(object):
    """
    a flow abstraction
    """
    def __init__(self, packet, duration=0.0):
        assert isinstance(packet, Packet), 'Wrong argument when initializing a flow'
        self.flow_id = self._geterate_flow_id()
        self.start_time = TimeUtils.time_convert(packet.packet_time)
        self.end_time = self.start_time + TimeUtils.time_delta_convert(duration)
        self.src_ip = packet.src_ip
        self.src_port = packet.src_port
        self.dst_ip = packet.dst_ip
        self.dst_port = packet.dst_port
        self.size = packet.packet_size

    def __str__(self):
        return "%s\t%s\t%s\t%s\t%s\t%s\t%d" % \
               (TimeUtils.time_to_string(self.start_time),
                self.duration.total_seconds(), self.src_ip, self.src_port, self.dst_ip, self.dst_port, self.size)

    @staticmethod
    def _generate_flow_id():
        #TODO: realize a unique flow_id generator
        return NotImplementedError()

    @property
    def duration(self):
        return self.end_time - self.start_time

    def get_flow_id(self):
        return self.flow_id

    def add_packet(self, packet, duration=0.0):
        """
        add a new packet to flow
        :param packet: an object of Packet
        :param duration: new packet's duration
        :return: None
        """
        assert isinstance(packet, Packet), 'Wrong argument when adding a packet to flow'
        self.end_time = TimeUtils.time_convert(packet.packet_time) + TimeUtils.time_delta_convert(duration)
        self.size += packet.packet_size