from twisted.internet.protocol import Protocol
from twisted.internet.protocol import ClientFactory
from twisted.internet.protocol import ServerFactory

from twisted.internet import reactor

from scapy.layers.l2 import Ether
from scapy.layers.inet import UDP, TCP
import time

from twink.ofp4 import parse
import traceback

import struct
import logging
import random
import sys

# logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(level=logging.ERROR)


# The program will exit when an exception happens
def exit_fwst():
    reactor.stop()
    sys.exit(1)


# Server: Handle the connections from switches
class OpenFlowServerProtocol(Protocol):
    def __init__(self):
        self.pending_bytes = 0
        self.data_buffer = ''

    def connectionMade(self):
        self.factory.add_switchConnection(self)
        logging.info("Receiving a switch connection")

    def connectionLost(self, reason):
        self.factory.remove_switchConnection(self)
        logging.info("Losing a switch connection!")

    def dataReceived(self, data):
        self.data_buffer += data
        while True:
            available_bytes = len(self.data_buffer)
            if self.pending_bytes == 0:
                if available_bytes < 8:
                    return
                parsed_header = struct.unpack(">bbHI", self.data_buffer[:8])
                self.pending_bytes = parsed_header[2]
            if self.pending_bytes > available_bytes:
                return
            openflow_msg = self.data_buffer[:self.pending_bytes]
            self.data_buffer = self.data_buffer[self.pending_bytes:]
            self.factory.handle_switch_openflow_msg(openflow_msg, self)
            self.pending_bytes = 0


# Client: Handle the connections from controllers
class OpenFlowClientProtocol(Protocol):
    def __init__(self):
        self.pending_bytes = 0
        self.data_buffer = ''

    def dataReceived(self, data):
        self.data_buffer += data
        while True:
            available_bytes = len(self.data_buffer)
            if self.pending_bytes == 0:
                if available_bytes < 8:
                    return
                parsed_header = struct.unpack(">bbHI", self.data_buffer[:8])
                self.pending_bytes = parsed_header[2]
            if self.pending_bytes > available_bytes:
                return
            openflow_msg = self.data_buffer[:self.pending_bytes]
            self.data_buffer = self.data_buffer[self.pending_bytes:]
            self.factory.handle_controller_openflow_msg(openflow_msg, self, self.factory.switchConn)
            self.pending_bytes = 0

    def connectionMade(self):
        logging.info("Connecting to a controller!")
        self.transport.setTcpNoDelay(True)
        self.factory.add_controllerConnection(self, self.factory.switchConn)
        hello_msg = self.factory.ofmsg_generator(0)
        self.transport.write(hello_msg)
        reactor.callLater(5, self.send_echo_request)

    def send_echo_request(self):
        echo_msg = self.factory.ofmsg_generator(2)
        self.transport.write(echo_msg)
        reactor.callLater(5, self.send_echo_request)

    def connectionLost(self, reason):
        logging.info("Losing a controller!")
        self.factory.remove_controllerConnection(self, self.factory.switchConn)
        exit_fwst()


class OpenFlowService():
    test = 1
    switch_to_controller = {}  # a dictionary: Switch_conn --> [a list of controllers]
    switch_reply_keeper = {}  # Switch_connection --> keeper, keeper is a dict : key(type-xid) --> an array of controller_connections.
    CONTROLLER_IPS = ["192.168.56.101", "192.168.56.102", "192.168.56.103"]
    CONTROLLER_REPLY_TYPES = [5, 7, 18, 20, 24, 26]  # reply from switch is required when receiveing these messages from controllers
    SWITCH_REPLY_TYPES = [6, 8, 19, 21, 25, 27]  # reply from switch corresponds to CONTROLLER_REPLY_TYPES
    switch_to_master = {}    # Switch_conn --> its master controller
    robin_round_factor = 0
    # udp_start_time = {}
    # udp_stop_time = []
    # tcp_start_time = {}
    # tcp_stop_time = []
    offset = time.time()
    testFile = open("log.txt", "wb")    # record the received time of packet_in and the received time of packet_out
    # CONTROLLER_REPLY_TYPES = [5, 7, 18, 20, 26]  # reply required when receiveing messages from controllers
    # SWITCH_REPLY_TYPES = [6, 8, 19, 21, 27]

    def add_switchConnection(self, conn):
        if conn not in self.switch_to_controller:
            self.switch_to_controller[conn] = []
            self.switch_reply_keeper[conn] = {}
        else:
            logging.warning("Unexpected situation!!!! Switch already in ")

    def remove_switchConnection(self, conn):
        if conn in self.switch_to_controller:
            del self.switch_to_controller[conn]
            del self.switch_reply_keeper[conn]
        else:
            logging.warning("Unexpected situation Switch not in when removing")

    def add_controllerConnection(self, controller_conn, switch_conn):
        if switch_conn in self.switch_to_controller:
            pool = self.switch_to_controller[switch_conn]
            if controller_conn not in pool:
                pool.append(controller_conn)
            else:
                logging.warning("Controller already in switch")
        else:
            logging.warning("Unexpected not such switch when adding controller")

    def remove_controllerConnection(self, controller_conn, switch_conn):
        if switch_conn in self.switch_to_controller:
            pool = self.switch_to_controller[switch_conn]
            if controller_conn in pool:
                pool.remove(controller_conn)
            else:
                logging.warning("Controller not in switch pool when removing ")
        else:
            logging.warning("Unexpected not such switch when removing controller")

    # find the controller according to the reply message received from the switch
    # according to the switch_conn and (msg_type, xid)
    def find_controller_given_reply(self, switch_conn, type, xid, hasmore=False):
        if switch_conn in self.switch_reply_keeper:
            temp = self.switch_reply_keeper[switch_conn]
            mykey = "%s-%s" % (type, xid)
            if mykey in temp:
                pool = temp[mykey]
                if not hasmore:
                    result = pool.pop()
                else:
                    result = pool[0]
                if len(pool) == 0:
                    del temp[mykey]
                return result
            else:
                logging.error("Unable to find a connection to reply")
                return None
        else:
            logging.warning("Unable to find switch in reply keeper")
            return None

    # record the controller connection when a reply is required from the switch
    # according to the switch_conn and (msg_type, xid)
    def record_controller_request(self, switch_conn, controller_conn, type, xid):
        if switch_conn in self.switch_reply_keeper:
            temp = self.switch_reply_keeper[switch_conn]
            mykey = "%s-%s" % (type, xid)
            if mykey in temp:
                pool = temp[mykey]
                pool.append(controller_conn)
            else:
                temp[mykey] = []
                temp[mykey].append(controller_conn)
        else:
            logging.warning("Unable to record controller as switch not in reply keeper")
            exit_fwst()

    # register the operation in factory to share among client_protocol instances
    def getOpenFlowClientFactory(self, switchConn):
        f = ClientFactory()
        f.protocol = OpenFlowClientProtocol
        f.switchConn = switchConn
        f.add_controllerConnection = self.add_controllerConnection
        f.handle_controller_openflow_msg = self.handle_controller_openflow_msg
        f.remove_controllerConnection = self.remove_controllerConnection
        f.ofmsg_generator = self.ofmsg_generator
        return f

    # register the operation in factory to share among server_protocol instances
    def getOpenFlowServerFactory(self):
        f = ServerFactory()
        f.protocol = OpenFlowServerProtocol
        f.handle_switch_openflow_msg = self.handle_switch_openflow_msg
        f.add_switchConnection = self.add_switchConnection
        f.remove_switchConnection = self.remove_switchConnection
        return f

    # generate/fabricate openflow message according to the required type, xid and data
    def ofmsg_generator(self, type, xid=0, data=''):
        if xid == 0:
            xid = random.randint(2, 65534000)
        version = 4
        length = 8 + len(data)
        msg = struct.pack(">bbHI", version, type, length, xid) + data
        return msg

    # is_special_packets means the packets are LLDP(88cc) and (0806), which are used to construct the network topology
    def is_special_packets(self, packet):
        raw_packet_type = packet[54:56]
        ptype = struct.unpack(">BB", raw_packet_type)
        stringtype = "%02x%02x" % ptype
        if stringtype == "88cc":
            return True
        elif stringtype == "0806":
            return True
        return False

    '''
    ======================================================================================
    Scheduling Algorithm
    ======================================================================================
    '''
    # this function is called when the program received Packet_in
    # If this packet is a special packet, it should be sent to its master for topology construction
    # Otherwise, the packet would be sent to any one of the controllers it connect
    def schedule(self, swiconn, isSpecialPacket):
        if isSpecialPacket:
            conn = self.find_master(swiconn)
            if conn is None:
                logging.error("We get a random one")
                return self.switch_to_controller[swiconn][0]
            return conn

        # '''
        # ===============================================================================
        #             All the packets forward to the first controller
        # ===============================================================================
        # '''
        # return self.switch_to_controller[swiconn][0]

        '''
        ===============================================================================
                    Robin-round
        ===============================================================================
        '''
        controller_num = len(self.switch_to_controller[swiconn])
        self.robin_round_factor = (self.robin_round_factor + 1) % controller_num
        return self.switch_to_controller[swiconn][self.robin_round_factor]

        # '''
        # ===============================================================================
        #             Packets are sent back to the master
        # ===============================================================================
        # '''
        # conn = self.find_master(swiconn)
        # if conn is None:
        #     logging.error("Packet_in can't find master, so We get a random one")
        #     return self.switch_to_controller[swiconn][0]
        # return conn
        #
        # '''
        # ===============================================================================
        #             Weighted Robin-round
        # ===============================================================================
        # '''

    # find the master controller given switch_conn
    def find_master(self, swiconn):
        if swiconn not in self.switch_to_master:
            return None
        return self.switch_to_master[swiconn]

    def handle_switch_openflow_msg(self, msg, swiconn):
        openflow_header = struct.unpack(">bbHI", msg[:8])
        type = openflow_header[1]
        xid = openflow_header[3]
        length = openflow_header[2]
        logging.debug("Received Switch message: Type %d xid:%d length:%d from switch:%s" % (
        type, xid, length, swiconn.transport.getPeer()))

        if type == 0:
            for ip in self.CONTROLLER_IPS:
                clientF = self.getOpenFlowClientFactory(swiconn)
                logging.debug("Established a connection to controller")
                reactor.connectTCP(ip, 6633, clientF)
            reply_msg = self.ofmsg_generator(0, xid)
            logging.debug("Fabricate a hello")
            swiconn.transport.write(reply_msg)
        elif type == 2:
            reply_msg = self.ofmsg_generator(3, xid, msg[8:])
            swiconn.transport.write(reply_msg)
            logging.debug("Fabricate a echo reply")

        elif type == 10:
            if swiconn not in self.switch_to_controller:
                logging.error("No controller given swi")
                exit_fwst()
            conn = self.schedule(swiconn, self.is_special_packets(msg))
            # if self.is_special_packets(msg):
            # print "Special Swi %s Controller %s" %(swiconn.transport.getPeer(), conn.transport.getPeer())
            if conn is None:
                logging.error("Not available schedule")
                exit_fwst()
            conn.transport.write(msg)
            # temp = self.switch_to_controller[swiconn]
            # temp[0].transport.write(msg)
            self.measure(msg)

        elif type == 25:
            body = struct.unpack(">IIQ", msg[8:])
            role = body[0]
            controller_conn = self.find_controller_given_reply(swiconn, type - 1, xid)
            logging.info(
                "Role %d request %s <==> %s" % (role, swiconn.transport.getPeer(), controller_conn.transport.getPeer()))
            if role == 2:
                self.switch_to_master[swiconn] = controller_conn
                logging.info("Updating Master Controller Information for %s  <==> %s" % (
                swiconn.transport.getPeer(), controller_conn.transport.getPeer()))
            controller_conn.transport.write(msg)

        elif type in self.SWITCH_REPLY_TYPES:
            hasMore = False
            if type == 19:
                body = struct.unpack(">HH", msg[8:12])
                if body[1] == 1:
                    hasMore = True
            controller_conn = self.find_controller_given_reply(swiconn, type - 1, xid, hasMore)
            if controller_conn is None:
                logging.error("Controller can not find...It must be a bugggggggggggg")
                exit_fwst()
            controller_conn.transport.write(msg)
        else:
            con = self.find_master(swiconn)
            if con is None:
                logging.error("No master!!!")
                exit_fwst()
            con.transport.write(msg)
            # temp = self.switch_to_controller[swiconn]
            # for t1 in temp:
            #     t1.transport.write(msg)

    def handle_controller_openflow_msg(self, msg, controller_conn, swi_conn):
        openflow_header = struct.unpack(">bbHI", msg[:8])
        type = openflow_header[1]
        xid = openflow_header[3]
        version = openflow_header[0]

        length = openflow_header[2]
        logging.debug("Received Controller message: Type %d xid:%d length:%d from controller %s to switch:%s" % (
        type, xid, length, controller_conn.transport.getPeer(), swi_conn.transport.getPeer()))
        if type == 0:
            logging.debug("It is a hello from controller")
        elif type == 3:
            logging.debug("It is an echo reply")
        # elif type == 24:
        #     body = struct.unpack(">IIQ", msg[8:])
        #     role = body[0]
        #     padding = body[1]
        #     generation_id = body[2]
        #     logging.info("FWST ask to be %s" % role)
        #     # role = 1
        #     role_reply = struct.pack(">IIQ", role, padding, generation_id)
        #     reply_msg = self.ofmsg_generator(25, xid, role_reply)
        #     controller_conn.transport.write(reply_msg)
        elif type in self.CONTROLLER_REPLY_TYPES:
            self.record_controller_request(swi_conn, controller_conn, type, xid)
            swi_conn.transport.write(msg)
        else:
            swi_conn.transport.write(msg)
        if type == 13:
            self.measure(msg)

    # record the packet_in received time and packet_out received time
    def measure(self,msg):
        print self.test
        self.test += 1
        t1 = time.time() - self.offset
        s = struct.pack("d", t1)
        self.testFile.write(s)
        length = struct.pack("i",len(msg))
        self.testFile.write(length)
        self.testFile.write(msg)
        self.testFile.flush()
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



s = OpenFlowService()
reactor.listenTCP(6633, s.getOpenFlowServerFactory())
logging.info("Start running server")
reactor.run()







