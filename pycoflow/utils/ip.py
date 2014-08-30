class IP(object):
    def __init__(self):
        self.hosts = {}
    def parse_hosts(self, hosts_file):
        with open(hosts_file) as f:
            hosts_lines = f.readlines()
            for hosts_line in hosts_lines:
                [host_name, ip] = hosts_line.split(' ')
                ip = ip[:-1]
                self.hosts[host_name] = ip
        return self.hosts

