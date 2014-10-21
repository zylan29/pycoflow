import binascii


def hex2ten(hex_number):
    return int(binascii.b2a_hex(hex_number), 16)


def hex_ip_to_ten_ip(hex_ip):
    ten_ip_list = map(hex2ten, hex_ip)
    return '.'.join(map(str, ten_ip_list))