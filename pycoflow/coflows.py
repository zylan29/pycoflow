from coflow import Coflow
from flow import LogicalFlow

class Coflows(object):
    """
    all coflows
    """
    def __init__(self):
        self.coflows = {}

    def __str__(self):
        return "\n".join(map(str, self.coflows.values()))

    def add_logical_flow(self, logical_flow):
        """
        add logical_flow to a coflow or create a new coflow
        :param logical_flow: LogicalFlow object parsed form a line spark's log file
        :return:
        """
        assert isinstance(logical_flow, LogicalFlow),  'Wrong argument when adding a logical_flow to a coflow'
        coflow_id=self._find_coflow(logical_flow)
        if not coflow_id:
            new_coflow = Coflow(logical_flow)
            self.coflows[new_coflow.get_coflow_id()] = new_coflow
        else:
            self.coflows[coflow_id].logical_flows[logical_flow.generate_logical_flow_id()] = logical_flow
        return NotImplementedError()

    def add_packet(self, packet):
        """
        add packet to a coflow or create a new one
        :param packet:
        :return:
        """
        coflow_id = self._find_coflow(packet)
        if coflow_id:
            self.coflows[coflow_id].add_packet(packet)

    def _find_coflow(self, logical_flow):
        """
        find a proper coflow to place this packet
        :param logical_flow:
        :return:
        """
        assert isinstance(logical_flow, LogicalFlow)
        if logical_flow.shuffle_id in self.coflows:
                return logical_flow.shuffle_id
        return None
        # return NotImplementedError()