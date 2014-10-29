from coflow import Coflow
from flow import LogicalFlow
from utils.time import TimeUtils


class Application(object):
    """
    all coflows
    """

    def __init__(self):
        self.coflows = {}
        self.coflow_ids = []
        self.threshold1 = TimeUtils.time_delta_convert(1)

    def __str__(self):
        return "\n".join(map(str, self.coflows.values()))

    def add_logical_flow(self, logical_flow):
        """
        add logical_flow to a coflow or create a new coflow
        :param logical_flow: LogicalFlow object parsed form a line spark's log file
        :return:
        """
        assert isinstance(logical_flow, LogicalFlow),  'Wrong argument when adding a logical_flow to a coflow'
        coflow_id = self._find_coflow(logical_flow)
        if not coflow_id:
            new_coflow = Coflow(logical_flow)
            new_coflow_id = new_coflow.get_coflow_id()
            self.coflows[new_coflow_id] = new_coflow
            self.coflow_ids.append(new_coflow_id)
        else:
            assert isinstance(self.coflows[coflow_id], Coflow)
            self.coflows[coflow_id].add_logical_flows(logical_flow)

    def add_packet(self, packet):
        """
        add packet to a coflow or create a new one
        :param packet:
        :return:
        """
        coflow_id = self._find_flow(packet)
        if coflow_id:
            self.coflows[coflow_id].add_realistic_flows(packet)
            return coflow_id
        else:
            return None

    def add_retransmit_packet(self, packet):
        coflow_id = self._find_flow(packet)
        if coflow_id:
            self.coflows[coflow_id].add_retransmit_flows(packet)

    def _find_coflow(self, logical_flow):
        """
        find a proper coflow to place this logical_flow
        :param logical_flow:
        :return:
        """
        assert isinstance(logical_flow, LogicalFlow)
        if logical_flow.stage_id in self.coflows:
                return logical_flow.stage_id
        return None

    def _find_flow(self, packet):
        """
        find a proper coflow to place this packet
        :param packet:
        :return:
        """
        for (coflow_id, coflow) in self.coflows.iteritems():
            assert isinstance(coflow, Coflow), "wrong argument when a proper coflow to place a packet"
            if str(int(coflow_id) % 63) == packet.stage_id:
                if coflow.contains(packet):
                    return coflow_id
        return None