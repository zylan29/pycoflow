from utils.time import TimeUtils
from packet import Packet


class Flow(object):
    """
    a flow abstraction
    """
    def __init__(self, start_time, src_ip, dst_ip, flow_size, src_port=-1, dst_port=-1, duration=0.0):
        #set src_port and dst_port to -1 if not available
        self.start_time = TimeUtils.time_convert(start_time)
        self.end_time = self.start_time + TimeUtils.time_delta_convert(duration)
        self.src_ip = src_ip
        self.src_port = src_port
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.size = flow_size

    def __str__(self):
        return "%s\t%s\t%s\t%s\t%s\t%d" % \
               (TimeUtils.time_to_string(self.start_time),
                self.src_ip, self.src_port, self.dst_ip, self.dst_port, self.size)

    @property
    def duration(self):
        return self.end_time - self.start_time


class LogicalFlow(Flow):
    """
    a logical flow parsed form spark's log
    """
    def __init__(self, start_time, src_ip, dst_ip, flow_size, src_port=-1, dst_port=-1, duration=0.0):
        super(self, LogicalFlow).__init__(start_time, src_ip, dst_ip, flow_size, src_port=-1, dst_port=-1, duration=0.0)

    @staticmethod
    def from_log_line(log_line):
        #TODO:parse a line from spark's log , return a LogicalFlow object
        return NotImplementedError()


class RealisticFlow(Flow):
    """
    a realistic flow parsed from captured packet
    """
    def __init__(self, packet, duration=0.0):
        assert isinstance(packet, Packet), 'Wrong argument when initializing a flow'
        super(self, RealisticFlow).__init__(packet.packet_time, packet.src_ip, packet.dst_ip,
                                            packet.packet_size, packet.src_port, packet.dst_port, duration)
        self.flow_id = self._geterate_flow_id()

    def __str__(self):
        return "%s\t%s\t%s\t%s\t%s\t%s\t%d" % \
               (TimeUtils.time_to_string(self.start_time),
                self.duration.total_seconds(), self.src_ip, self.src_port, self.dst_ip, self.dst_port, self.size)

    @staticmethod
    def _generate_flow_id():
        #TODO: realize a unique flow_id generator
        return NotImplementedError()

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