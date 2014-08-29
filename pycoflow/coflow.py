from flow import RealisticFlow
from packet import Packet
from flow import LogicalFlow


class Coflow(object):
    """
    a coflow abstraction
    """
    def __init__(self, logical_flow):
        assert isinstance(logical_flow, LogicalFlow),  'Wrong argument when initializing a coflow with a logical_flow'
        self.coflow_id = self._generate_coflow_id(logical_flow)
        self.logical_flows = {logical_flow.generate_logical_flow_id: logical_flow}
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
        #将shuffle_id作为coflow_id
        return logical_flow.shuffle_id
        # return NotImplementedError()

    def get_coflow_id(self):
        return self.coflow_id

    def add_packet(self, packet):
        """
        add packet to a flow in this coflow or create a new flow by the packet
        :param packet: an object of Packet
        :return:
        """
        assert isinstance(packet, Packet),  'Wrong argument when adding a packet to coflow'
        flow_id = self._find_flow(packet)
        if not flow_id:
            new_flow = RealisticFlow(packet)
            self.realistic_flows[new_flow.get_flow_id()] = new_flow
        else:
            self.realistic_flows[flow_id].add_packet(packet)

    def _find_flow(self, packet):
        """
        find a proper flow to place this packet
        :param packet: an object of Packet
        :return: if found return flow_id, else return None
        """

        #查找logical_flow中的流，如果能找到与之相匹配的，则返回logical_flow的id
        #具体的匹配特征的方法还没想到

        #TODO: realize this function
        return NotImplementedError()