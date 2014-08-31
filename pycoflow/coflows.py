from coflow import Coflow
from flow import LogicalFlow
import datetime

class Coflows(object):
    """
    all coflows
    """

    def __init__(self):
        self.coflows = {}
        self.threshold1 = datetime.timedelta(0, 2)
        self.threshold2 = datetime.timedelta(0, 0, 10)

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
            self.coflows[new_coflow.get_coflow_id()] = new_coflow
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

    def _find_coflow(self, logical_flow):
        """
        find a proper coflow to place this logical_flow
        :param logical_flow:
        :return:
        """
        assert isinstance(logical_flow, LogicalFlow)
        if logical_flow.shuffle_id in self.coflows:
                return logical_flow.shuffle_id
        return None
        # return NotImplementedError()

    def _find_flow(self, packet):
        """
        find a proper coflow to place this packet
        :param packet:
        :return:
        """
        #TODO: compare packet with coflows[*].logical_flows[**],find *
        for (k, v) in self.coflows.iteritems():
            assert isinstance(v, Coflow), "wrong argument when a proper coflow to place a packet"
            for (k1, v1) in v.logical_flows.iteritems():
                if packet.dst_ip == v1.dst_ip and packet.dst_port == v1.dst_port \
                        and v1.start_time <= packet.packet_time <= v1.end_time+self.threshold1:
                    return k
        return None
