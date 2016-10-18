import struct
import traceback

from scapy.layers.inet import UDP
from scapy.layers.l2 import Ether
from twink.ofp4 import parse

testFile = open("log.txt", "rb")
udp_start_time = {}
udp_stop_time = []
data = []
while True:
    try:

        temp = testFile.read(8)
        if temp is None:
            break
        start = struct.unpack("d",temp)[0]
        temp = testFile.read(4)
        length = struct.unpack("i",temp)[0]
        msg = testFile.read(length)
        data.append((start, length, msg))
    except:
        traceback.print_exc()
        break

for (start, length, msg) in data:
    pkt = parse.parse(msg)
    print "working start%s"%start
    if pkt.__class__.__name__ == "ofp_packet_in":
        ether_pkt = Ether(pkt.data)
        if UDP in ether_pkt:
            udp_sport = ether_pkt[UDP].sport
            start_time = start
            udp_start_time[udp_sport]=start_time


    elif pkt.__class__.__name__ == "ofp_packet_out":
        ether_pkt = Ether(pkt.data)
        if UDP in ether_pkt:
            stop_time = start
            udp_sport = ether_pkt[UDP].sport
            udp_stop_time.append(stop_time - udp_start_time[udp_sport])


print len(udp_stop_time)
print sum(udp_stop_time)/len(udp_stop_time)



        # try:
        #     pkt = parse.parse(msg)
        #
        #     if pkt.__class__.__name__ == "ofp_packet_in":
        #         ether_pkt = Ether(pkt.data)
        #         if UDP in ether_pkt:
        #             udp_sport = ether_pkt[UDP].sport
        #             start = time.time()
        #             self.udp_start_time[udp_sport]=start
        #         # elif TCP in ether_pkt:
        #         #     tcp_sport = ether_pkt[TCP].sport
        #         #     start = time.time()
        #         #     self.tcp_start_time[tcp_sport]=start
        #     elif pkt.__class__.__name__ == "ofp_packet_out":
        #         ether_pkt = Ether(pkt.data)
        #         if UDP in ether_pkt:
        #             stop = time.time()
        #             udp_sport = ether_pkt[UDP].sport
        #             self.udp_stop_time.append(stop - self.udp_start_time[udp_sport])
        #             # print stop - self.udp_start_time[udp_sport]
        #             print len(self.udp_stop_time)
        #             print sum(self.udp_stop_time)/len(self.udp_stop_time)
        #     else:
        #         print "unexpected"
        #         # elif TCP in ether_pkt:
        #         #     stop = time.time()
        #         #     tcp_sport = ether_pkt[TCP].sport
        #         #     self.tcp_stop_time.append(stop - self.tcp_start_time[tcp_sport])
        #         #     print len(self.tcp_stop_time)
        #         #     print sum(self.tcp_stop_time) / len(self.tcp_stop_time)
        # except:
        #     traceback.print_exc()