# from scapy.all import *
# from scapy.layers.inet import UDP, IP, TCP
# from scapy.layers.l2 import Ether, sendp
# from scapy.all import *
import socket
import time

# pkt = []
#
# for i in range(1,5000):
#
#
# for i in range(1,5000):
#     pkt.append(Ether()/IP(dst="10.0.0.1")/TCP(sport=i,dport=8080))
#     print i

i = 30000
while True:

    i += 1
    if i > 60000:
        break
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("10.0.0.2", i))
    sock.sendto("Hello", ("10.0.0.1",8080))
    time.sleep(0.001)
    print "Hello %s"%i

# import socket
# import sys
# from time import sleep
#
# # address = (sys.argv[1], 9999)
# address = ('10.0.0.1',8080)
# s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
# i = 1
#
# while i < 100:
#     s.sendto('hi : %d\n' % i, address)
#     i += 1
#     sleep(3)
#     print i
# s.close()
