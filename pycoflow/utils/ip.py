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


def host2ip(host_name, hosts_file=''):
    if hosts == {}:
        assert hosts_file != '', "No hosts file specified!"
        parse_hosts(hosts_file)
    else:
        return hosts[host_name]