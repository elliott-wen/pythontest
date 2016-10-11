__author__ = 'Victoria'

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.log import setLogLevel, info
from mininet.node import RemoteController
from mininet.cli import CLI
from mininet.util import dumpNodeConnections

"""
Instructions to run the topo:
    1. Go to directory where this fil is.
    2. run: sudo -E python My_Topo.py

The topo has 3 switches and 2 hosts. They are connected in a line shape.
"""


class My_Topo(Topo):
    """Simple topology example."""

    def __init__(self, m=3, n=2, **opts):
        # Create custom topo.
        # Switches Number = m.
        # Hosts Number for each switch = n

        # Initialize topology
        # It uses the constructor for the Topo class
        super(My_Topo, self).__init__(**opts)

        for i in range(m):
            switch = self.addSwitch('s%s'%(i+1))
            if i > 0:
                self.addLink(switch,temp)
            for j in range(n):
                host = self.addHost('h%s'%(j+1 + (i+1)*10))
                self.addLink(host,switch)
            temp = switch


def run():

    c = RemoteController('c', '127.0.0.1', 6633)
    #c = RemoteController('c', '127.0.0.1', 6633) # scheduler works as a remote controller

    topo = My_Topo()
    net = Mininet(topo=topo, controller=None)
    net.addController(c)
    net.start()
    print("Dumping host connections")
    dumpNodeConnections(net.hosts)
    print("Testing network connectivity")
    net.pingAll()

    CLI(net)
    net.stop()

# if the script is run directly (sudo custom/optical.py):
if __name__ == '__main__':
    setLogLevel('info')
    run()

