from utils.time import TimeUtils
from utils.ip import get_ip
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


class LogicalFlow():
    """
    a logical flow parsed form spark's log
    """
    def __init__(self, start_time, shuffle_id, reduce_id, blocks, flow_size, src_name, src_port):
        self.start_time = start_time
        self.shuffle_id = shuffle_id
        self.reduce_id = reduce_id
        self.blocks = blocks
        self.size = flow_size
        self.src_ip = get_ip(src_name)
        self.src_port = src_port

    def generate_logical_flow_id(self):
        return +self.src_ip+':'+self.src_port

    def append_logical_flow(self, logical_flow):
        assert isinstance(logical_flow, LogicalFlow), "Wrong argument when appending a logical_flow"
        self.blocks += logical_flow.blocks
        self.size += logical_flow.size

    @staticmethod
    def from_log_line(log_line):
        #TODO:parse a line from spark's log , return a LogicalFlow object
        try:
            fields = log_line.split(' ')
            temp1 = fields[2].split('=')
            temp2 = fields[3].split('=')
            temp3 = fields[4].split('=')
            temp4 = fields[5].split('=')
            temp5 = fields[6].split('=')
            temp6 = temp5[1].split(':')
            [start_time, shuffle_id, reduce_id, blocks, size, src_name, src_port] =\
                [fields[0]+' '+fields[1], temp1[1], temp2[1], temp3[1],temp4[1],
                 temp6[0],temp6[1]]
            if len(start_time) == 16 and start_time.endswith("000"):
                start_time = start_time[:-3]
            start_time = TimeUtils.time_convert(start_time)
            size = int(size)
        except ValueError:
            return None
        else:
            return LogicalFlow(start_time, shuffle_id, reduce_id, blocks, size, src_name, src_port)


class RealisticFlow(Flow):
    """
    a realistic flow parsed from captured packet
    """
    def __init__(self, packet, duration=0.0):
        assert isinstance(packet, Packet), 'Wrong argument when initializing a flow'
        super(RealisticFlow, self).__init__(packet.packet_time, packet.src_ip, packet.dst_ip,
                                            packet.packet_size, packet.src_port, packet.dst_port, duration)
        self.flow_id = self._generate_flow_id(packet)

    def __str__(self):
        return "%s\t%s\t%s\t%s\t%s\t%s\t%d" % \
               (TimeUtils.time_to_string(self.start_time),
                self.duration.total_seconds(), self.src_ip, self.src_port, self.dst_ip, self.dst_port, self.size)

    @staticmethod
    def _generate_flow_id(packet):
        #TODO: realize a unique flow_id generator
        return packet.src_ip+':'+packet.src_port
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