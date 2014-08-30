class IP(object):
    def _init_(self):
        self.hosts={}

    def parse_hosts(self, hosts_file):
        with open(hosts_file) as f:
            hosts_lines = f.readlines()
            for hosts_line in hosts_lines:
                [host_name, ip] = hosts_line.split(' ')
                ip = ip[:-1]
                self.hosts[host_name] = ip

    @staticmethod
    def get_ip( host_name):
        return IP.hosts[host_name]

