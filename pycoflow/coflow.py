from packet import Packet
from flow import LogicalFlow
from flow import RealisticFlow
from utils.time import TimeUtils


class Coflow(object):
    """
    a coflow abstraction
    """
    def __init__(self, logical_flow):
        assert isinstance(logical_flow, LogicalFlow), 'Wrong argument when initializing a coflow with a logical_flow'
        self.start_time = logical_flow.start_time
        self.end_time = self.start_time
        self.coflow_id = self._generate_coflow_id(logical_flow)
        self.logical_flows = {logical_flow.generate_logical_flow_id(): logical_flow}
        self.realistic_flows = {}

    def __str__(self):
        return "Coflow " + str(self.coflow_id) + ":\n" + TimeUtils.time_to_string(self.start_time) + " " + \
               str((self.end_time - self.start_time).total_seconds()) + '\n'+ "Logical Flows:\n" + \
               "\n".join(map(str, self.logical_flows.values())) + "\n" + "Real Flows:\n" + \
               "\n".join(map(str, self.realistic_flows.values()))

    @staticmethod
    def _generate_coflow_id(logical_flow):

        """
        :param logical_flow:
        :return:a unique coflow_id
        """
        return logical_flow.shuffle_id

    def get_coflow_id(self):
        return self.coflow_id

    def add_logical_flows(self, logical_flow):
        """
        add logical_flow to logical_flows(dict)
        :param logical_flow:
        :return:
        """
        assert isinstance(logical_flow, LogicalFlow),  'Wrong argument when adding a logical_flow to logical_flows'
        for (k, v) in self.logical_flows.iteritems():
            if logical_flow.dst_ip == v.dst_ip and logical_flow.dst_port == v.dst_port and logical_flow.src_ip == v.src_ip:
                if logical_flow.reduce_id == v.reduce_id:
                    print "WARN: logical flow duplicates:"
                    print logical_flow
                else:
                    v.append_logical_flow(logical_flow)
                return
        self.logical_flows[logical_flow.generate_logical_flow_id()] = logical_flow

    def add_realistic_flows(self, packet):
        """
        add packet to a realistic_flow in this coflow or create a new realistic_flow by the packet
        :param packet: an object of Packet
        :return:
        """
        assert isinstance(packet, Packet),  'Wrong argument when adding a packet to coflow'
        flow_id = self._find_realistic_flow(packet)
        if not flow_id:
            for logical_flow in self.logical_flows.values():
                if packet.dst_ip == logical_flow.dst_ip and packet.dst_port == logical_flow.dst_port:
                    new_flow = RealisticFlow(packet)
                    self.realistic_flows[new_flow.get_flow_id()] = new_flow
                    return
        else:
            self.realistic_flows[flow_id].add_packet(packet)

    def _find_realistic_flow(self, packet):
        """
        find a proper realistic_flow to place this packet
        :param packet: an object of Packet
        :return: if found return flow_id, else return None
        """
        assert isinstance(packet, Packet),  'Wrong argument when finding a packet to realistic_flow'
        for flow_id in self.realistic_flows:
            flow = self.realistic_flows[flow_id]
            if packet.dst_ip == flow.dst_ip and packet.dst_port == flow.dst_port and packet.src_ip == flow.src_ip:
                return flow_id
        return None

    def add_retransmit_flows(self, packet):
        flow_id = self._find_realistic_flow(packet)
        if flow_id:
            self.realistic_flows[flow_id].add_retransmit_packet(packet)