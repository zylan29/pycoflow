from packet import Packet


IP_FILTER = ["192.168.1.1", "192.168.1.12"]
PORT_FILTER = ["22"]
SHUFFLE_ID_FILTER = ["-1"]
SIZE_FILTER = 52


def packet_filter(packet):
    assert isinstance(packet, Packet), "Argument should be an instance of Packet"
    if packet.stage_id in SHUFFLE_ID_FILTER:
        return True
    if packet.dst_ip in IP_FILTER or packet.src_ip in IP_FILTER:
        return True
    if packet.src_port in PORT_FILTER or packet.dst_port in PORT_FILTER:
        return True
    if packet.packet_size <= SIZE_FILTER:
        return True
    if packet.packet_size in [56, 88, 90, 97]:
        return True
    return False