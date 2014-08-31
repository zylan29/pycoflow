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


class LogicalFlow():
    """
    a logical flow parsed form spark's log
    """
    def __init__(self, start_time, shuffle_id, reduce_id, blocks, flow_size, dst_ip, dst_port, src_ip):
        self.start_time = start_time
        self.shuffle_id = shuffle_id
        self.reduce_id = reduce_id
        self.blocks = blocks
        self.size = flow_size
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.end_time = start_time
        self.src_ip = src_ip

    def generate_logical_flow_id(self):
        return TimeUtils.to_string(self.start_time)+"--"+TimeUtils.to_string(self.end_time)+" "+ \
               self.src_ip+"->"+self.dst_ip+':'+self.dst_port

    def append_logical_flow(self, logical_flow):
        assert isinstance(logical_flow, LogicalFlow), "Wrong argument when appending a logical_flow"
        self.blocks += logical_flow.blocks
        self.size += logical_flow.size
        self.start_time = self.start_time if self.start_time < logical_flow.start_time else logical_flow.start_time
        self.end_time = self.end_time if self.end_time > logical_flow.start_time else logical_flow.start_time

    def __str__(self):
        return "%s\t%s:%s\t%d" % \
               (TimeUtils.time_to_string(self.start_time), self.dst_ip, self.dst_port, self.size)

    @staticmethod
    def from_log_line(log_line, hosts):
        #TODO:parse a line from spark's log , return a LogicalFlow object
        try:
            fields = log_line.split(' ')
            [start_time, shuffle_id, reduce_id, blocks, size, dst_name, dst_port, src_ip] =\
                [fields[0]+' '+fields[1], fields[2].split('=')[1], fields[3].split('=')[1], fields[4].split('=')[1],
                 fields[5].split('=')[1], fields[6].split('=')[1].split(':')[0], fields[6].split('=')[1].split(':')[1],
                 fields[7][:-1]]
            if len(start_time) == 21:
                start_time += "000"
            start_time = TimeUtils.time_convert(start_time)
            dst_ip = hosts[dst_name]
            size = int(size)
            blocks = int(blocks)
        except ValueError:
            return None
        else:
            return LogicalFlow(start_time, shuffle_id, reduce_id, blocks, size, dst_ip, dst_port, src_ip)


class RealisticFlow(Flow):
    """
    a realistic flow parsed from captured packet
    """
    def __init__(self, packet, duration=0.0):
        assert isinstance(packet, Packet), 'Wrong argument when initializing a flow'
        super(RealisticFlow, self).__init__(packet.packet_time, packet.src_ip, packet.dst_ip,
                                            packet.packet_size, packet.src_port, packet.dst_port, duration)
        self.flow_id = self._generate_flow_id(packet)
        self.packet_num = 1

    def __str__(self):
        return "%s\t%s\t%s:%s--->%s:%s\t%d" % \
               (TimeUtils.time_to_string(self.start_time),
                self.duration.total_seconds(), self.src_ip, self.src_port, self.dst_ip, self.dst_port, self.size)

    @staticmethod
    def _generate_flow_id(packet):
        #TODO: realize a unique flow_id generator
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
        self.start_time = self.start_time if self.start_time < packet.packet_time else packet.packet_time
        self.end_time = TimeUtils.time_convert(packet.packet_time) + TimeUtils.time_delta_convert(duration)
        self.size += packet.packet_size
        self.packet_num += 1