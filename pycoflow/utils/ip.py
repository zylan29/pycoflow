hosts={}
@staticmethod
def parse_hosts(hosts_file):
    with open(hosts_file) as f:
        hosts_lines=f.readlines()
        for hosts_line in hosts_lines:
            [host_name, ip] = hosts_line.split('/t')
            hosts[host_name] = ip

def get_ip(host_name):
    return hosts[host_name]

