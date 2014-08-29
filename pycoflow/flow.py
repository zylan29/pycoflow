from utils.time import TimeUtils
from packet import Packet
from coflow_parse import CoflowParse


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


class LogicalFlow():
    """
    a logical flow parsed form spark's log
    """
    def __init__(self, start_time, shuffle_id, reduce_id, blocks, size, src_name, src_port ):
        self.start_time = start_time
        self.shuffle_id = shuffle_id
        self.reduce_id = reduce_id
        self.blocks = blocks
        self.size = size
        self.src_ip = CoflowParse.get_ip(src_name)
        self.src_port = src_port

    def generate_logical_flow_id(self):
        return self.start_time+self.src_ip+':'+self.src_port

    @staticmethod
    def from_log_line(log_line):
        #TODO:parse a line from spark's log , return a LogicalFlow object
        # try:
            # [start_time, shuffle_id, reduce_id, blocks, size, src_name, src_port ]=还不知道该如何把日志文件的一行中的信息取出
            # if len(start_time) == 16 and packet_time.endswith("000"):
            #     start_time = start_time[:-3]
            # start_time = TimeUtils.time_convert(packet_time)
            # size = int(size)

        # except ValueError:
        #     return None
        # else:
            # return LogicalFlow( start_time,shuffle_id, reduce_id, blocks, size, src_name, src_port )
        return NotImplementedError()


class RealisticFlow(Flow):
    """
    a realistic flow parsed from captured packet
    """
    def __init__(self, packet, duration=0.0):
        assert isinstance(packet, Packet), 'Wrong argument when initializing a flow'
        super(self, RealisticFlow).__init__(packet.packet_time, packet.src_ip, packet.dst_ip,
                                            packet.packet_size, packet.src_port, packet.dst_port, duration)
        self.flow_id = self._generate_flow_id(packet)

    def __str__(self):
        return "%s\t%s\t%s\t%s\t%s\t%s\t%d" % \
               (TimeUtils.time_to_string(self.start_time),
                self.duration.total_seconds(), self.src_ip, self.src_port, self.dst_ip, self.dst_port, self.size)

    @staticmethod
    def _generate_flow_id(packet):
        #TODO: realize a unique flow_id generator
        #用与生成logical_flow_id相同的的方法，保持两者id一致
        return packet.start_time+packet.src_ip+':'+packet.src_port
        # return NotImplementedError()

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