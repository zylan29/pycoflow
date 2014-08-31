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
        return "Coflow " + self.coflow_id + ":\n" + "\n".join(map(str, self.realistic_flows.values()))

    @staticmethod
    def _generate_coflow_id(logical_flow):

        """
        :param logical_flow:
        :return:a unique coflow_id
        """
        #TODO: realize a unique coflow_id generator
        return logical_flow.shuffle_id
        # return NotImplementedError()

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
                v.append_logical_flow(logical_flow)
                del self.logical_flows[k]
                self.logical_flows[v.generate_logical_flow_id()] = v
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
