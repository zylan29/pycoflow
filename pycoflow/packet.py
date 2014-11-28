from utils.time import TimeUtils


class Packet(object):
    """
    a packet abstraction
    """
    def __init__(self, shuffle_id, packet_time, src_ip, src_port, dst_ip, dst_port, packet_size):
        self.stage_id = str(shuffle_id)
        self.packet_time = packet_time
        self.src_ip = src_ip
        self.src_port = str(src_port)
        self.dst_ip = dst_ip
        self.dst_port = str(dst_port)
        self.packet_size = packet_size

    def __str__(self):
        return str(self.stage_id) + " " + TimeUtils.time_to_string(self.packet_time) + " "\
                + self.src_ip + ":" + self.src_port + " " + self.dst_ip + ":" + self.dst_port\
                + " " + str(self.packet_size)

    @classmethod
    def from_line_str(cls, flow_line):
        """
        get packet object from a line in captured file.
        :param flow_line: a line in captured file.
        :return: a packet object
        """
        try:
            [shuffle_code, packet_time, src_ip, src_port, dst_ip, dst_port, packet_size] = flow_line.split("\t")
            stage_id = str((int(shuffle_code) / 4) - 1)
            packet_time = TimeUtils.time_convert(packet_time)
            packet_size = int(packet_size)
        except ValueError:
            return None
        else:
            return cls(stage_id, packet_time, src_ip, src_port, dst_ip, dst_port, packet_size)