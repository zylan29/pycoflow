__author__ = 'anzigly'
from coflow import Coflow


class Coflows(object):
    """
    all coflows
    """
    def __init__(self):
        self.coflows = {}

    def add_packet(self, packet):
        """
        add packet to a coflow or create a new one
        :param packet:
        :return:
        """
        coflow_id = self._find_coflow(packet)
        if not coflow_id:
            new_coflow = Coflow()
            self.coflows[new_coflow.get_coflow_id()] = new_coflow
        else:
            self.coflows[coflow_id].add_packet(packet)

    def _find_coflow(self, packet):
        """
        find a proper coflow to place this packet
        :param packet:
        :return:
        """
        return NotImplementedError()