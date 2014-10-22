from packet import Packet


IP_FILTER = ["192.168.1.1", "192.168.1.12"]
PORT_FILTER = ["22"]
SHUFFLE_ID_FILTER = ["0", "-1"]


def packet_filter(packet):
    assert isinstance(packet, Packet), "Argument should be an instance of Packet"
    if packet.shuffle_id in SHUFFLE_ID_FILTER:
        return True
    if packet.dst_ip in IP_FILTER or packet.src_ip in IP_FILTER:
        return True
    if packet.src_port in PORT_FILTER or packet.dst_port in PORT_FILTER:
        return True
    return False