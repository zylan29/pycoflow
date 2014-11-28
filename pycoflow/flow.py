from utils.time import TimeUtils
from utils.ip import host2ip
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
    def __init__(self, start_time, stage_id, blocks, flow_size, dst_ip, dst_port, src_ip, app_id=''):
        self.start_time = start_time
        self.stage_id = stage_id
        self.blocks = blocks
        self.size = flow_size
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.end_time = start_time
        self.src_ip = src_ip
        self.app_id = app_id

    def __str__(self):
        return "%s %s %s %s:%s %d" % \
               (TimeUtils.time_to_string(self.start_time), self.stage_id, self.src_ip,
                self.dst_ip, self.dst_port, self.size)

    def generate_logical_flow_id(self):
        return self.stage_id + " " + self.src_ip+"->"+self.dst_ip+':'+self.dst_port

    def append_logical_flow(self, logical_flow):
        assert isinstance(logical_flow, LogicalFlow), "Wrong argument when appending a logical_flow"
        self.blocks += logical_flow.blocks
        self.size += logical_flow.size
        self.start_time = self.start_time if self.start_time < logical_flow.start_time else logical_flow.start_time
        self.end_time = self.end_time if self.end_time > logical_flow.start_time else logical_flow.start_time

    def output_flow(self, coflow_id, start_time):
        out_str = ' '.join(map(str, [coflow_id,
                                     (self.start_time-start_time).total_seconds(),
                                     self.src_ip, self.dst_ip, self.size]))
        return out_str

    @classmethod
    def from_log_line(cls, src_ip, log_line):
        fields = log_line.strip().split(' ')
        app_id = fields[0].split('/')[5].strip()
        start_time_str = fields[0].split(":")[1] + " " + fields[1]
        start_time = TimeUtils.time_convert(start_time_str)
        [stage_id, blocks, size, dst] = map(lambda x: x.split("=")[1], fields[5:])
        dst_name, dst_port = dst.split(':')
        dst_ip = host2ip(dst_name)
        size = int(size)
        blocks = int(blocks)
        return cls(start_time, stage_id, blocks, size, dst_ip, dst_port, src_ip, app_id)


class RealisticFlow(Flow):
    """
    a realistic flow parsed from captured packet
    """
    def __init__(self, packet, duration=0.0):
        assert isinstance(packet, Packet), 'Wrong argument when initializing a flow'
        super(RealisticFlow, self).__init__(packet.packet_time, packet.src_ip, packet.dst_ip,
                                            packet.packet_size, packet.src_port, packet.dst_port, duration)
        self.src_port = packet.src_port
        self.flow_id = self._generate_flow_id(packet)
        self.packet_num = 1
        self.retransmit_bytes = 0

    def __str__(self):
        return "%s\t%s\t%s:%s->%s:%s\t%d\t%d" % \
               (TimeUtils.time_to_string(self.start_time),
                self.duration.total_seconds(), self.src_ip, self.src_port, self.dst_ip, self.dst_port,
                self.size, self.retransmit_bytes)

    @staticmethod
    def _generate_flow_id(packet):
        return packet.src_ip+':'+packet.src_port+"->"+packet.dst_ip+':'+packet.dst_port

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
        assert packet.src_port == self.src_port, 'src_port number should be the same'
        self.start_time = self.start_time if self.start_time < packet.packet_time else packet.packet_time
        self.end_time = TimeUtils.time_convert(packet.packet_time) + TimeUtils.time_delta_convert(duration)
        self.size += packet.packet_size
        self.packet_num += 1

    def output_flow(self, coflow_id, start_time):
        out_str = ' '.join(map(str, [coflow_id, (self.start_time - start_time).total_seconds(), self.src_ip, self.src_port,
                                     self.dst_ip, self.dst_port, self.size]))
        return out_str

    def add_retransmit_packet(self, packet):
        self.retransmit_bytes += packet.packet_size
