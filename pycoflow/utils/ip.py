hosts = {}


def parse_hosts(hosts_file):
    with open(hosts_file) as f:
        hosts_lines = map(lambda x: x.strip(), f.readlines())
        for hosts_line in hosts_lines:
            if hosts_line.startswith("#") or hosts_line == '':
                continue
            ip_hosts = map(lambda x: x.strip(), hosts_line.split())
            ip = ip_hosts[0]
            host_names = ip_hosts[1:]
            for host_name in host_names:
                hosts[host_name] = ip


def host2ip(host_name, hosts_file='/etc/hosts'):
    if hosts == {}:
        parse_hosts(hosts_file)
    return hosts[host_name]


def ip2int(ip):
    ip_int = 0x0
    for ip_elem in map(int, ip.split('.')):
        ip_int = ip_int * 256 + ip_elem
    return ip_int