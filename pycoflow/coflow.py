from flow import Flow
from packet import Packet


class Coflow(object):
    """
    a coflow abstraction
    """
    def __init__(self):
        self.coflow_id = self._generate_coflow_id()
        self.flows = {}

    def __str__(self):
        return "\n".join(map(str, self.flows.values()))

    @staticmethod
    def _generate_coflow_id():
        #TODO: realize a unique coflow_id generator
        return NotImplementedError()

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
            new_flow = Flow(packet)
            self.flows[new_flow.get_flow_id()] = new_flow
        else:
            self.flows[flow_id].add_packet(packet)

    def _find_flow(self, packet):
        """
        find a proper flow to place this packet
        :param packet: an object of Packet
        :return: if found return flow_id, else return None
        """
        #TODO: realize this function
        return NotImplementedError()