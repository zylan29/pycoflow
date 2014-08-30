from packet import Packet
from flow import LogicalFlow
from flow import RealisticFlow


class Coflow(object):
    """
    a coflow abstraction
    """
    def __init__(self, logical_flow):
        assert isinstance(logical_flow, LogicalFlow), 'Wrong argument when initializing a coflow with a logical_flow'
        self.coflow_id = self._generate_coflow_id(logical_flow)
        self.logical_flows = {logical_flow.generate_logical_flow_id(): logical_flow}
        self.realistic_flows = {}

    def __str__(self):
        return "\n".join(map(str, self.realistic_flows.values()))

    @staticmethod
    def _generate_coflow_id(logical_flow):

        """
        :param logical_flow:
        :return:
        """
        #TODO: realize a unique coflow_id generator
        return logical_flow.shuffle_id
        # return NotImplementedError()

    def get_coflow_id(self):
        return self.coflow_id

    def add_logical_flows(self, logical_flow):
        """
        add logical_flow to logical_flows
        :param logical_flow:
        :return:
        """
        assert isinstance(logical_flow, LogicalFlow),  'Wrong argument when adding a logical_flow to logical_flows'
        for k in self.logical_flows:
            if logical_flow.generate_logical_flow_id() == k:
                self.logical_flows[k].append_logical_flow(logical_flow)
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
            new_flow = RealisticFlow(packet)
            self.realistic_flows[new_flow.get_flow_id()] = new_flow
        else:
            self.realistic_flows[flow_id].add_packet(packet)

    def _find_realistic_flow(self, packet):
        """
        find a proper realistic_flow to place this packet
        :param packet: an object of Packet
        :return: if found return flow_id, else return None
        """
        #TODO: realize this function
        assert isinstance(packet, Packet),  'Wrong argument when finding a packet to realistic_flow'
        for k in self.realistic_flows:
            if packet.src_ip+':'+packet.src_port+'-'+'>'+packet.dst_ip+':'+packet.dst_port == k:
                return k
        return None
