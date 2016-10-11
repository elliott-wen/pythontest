from twisted.internet.protocol import Protocol
from twisted.internet.protocol import ClientFactory
from twisted.internet.protocol import ServerFactory

from twisted.internet import reactor

import struct
import logging
import random
import sys

logging.basicConfig(level=logging.DEBUG)
# logging.basicConfig(level=logging.ERROR)


def exit_fwst():
    reactor.stop()
    sys.exit(1)

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
    switch_to_controller = {}
    switch_reply_keeper = {}  # Use a switch connection to get the keeper, keeper is a dict from key(type-xid) to an array of controller connections.
    CONTROLLER_IPS = ["10.0.3.7", "10.0.3.254"]
    CONTROLLER_REPLY_TYPES = [5, 7, 18, 20, 24, 26]  # reply required when receiveing messages from controllers
    SWITCH_REPLY_TYPES = [6, 8, 19, 21, 25, 27]
    switch_to_master = {}
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

    def find_controller_given_reply(self, switch_conn, type, xid, hasmore = False):
        if switch_conn in self.switch_reply_keeper:
            temp = self.switch_reply_keeper[switch_conn]
            mykey = "%s-%s"%(type, xid)
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

    def record_controller_request(self, switch_conn, controller_conn, type, xid):
        if switch_conn in self.switch_reply_keeper:
            temp = self.switch_reply_keeper[switch_conn]
            mykey = "%s-%s"%(type, xid)
            if mykey in temp:
                pool = temp[mykey]
                pool.append(controller_conn)
            else:
                temp[mykey] = []
                temp[mykey].append(controller_conn)
        else:
            logging.warning("Unable to record controller as switch not in reply keeper")
            exit_fwst()

    def getOpenFlowClientFactory(self, switchConn):
        f = ClientFactory()
        f.protocol = OpenFlowClientProtocol
        f.switchConn = switchConn
        f.add_controllerConnection = self.add_controllerConnection
        f.handle_controller_openflow_msg = self.handle_controller_openflow_msg
        f.remove_controllerConnection = self.remove_controllerConnection
        f.ofmsg_generator = self.ofmsg_generator
        return f

    def getOpenFlowServerFactory(self):
        f = ServerFactory()
        f.protocol = OpenFlowServerProtocol
        f.handle_switch_openflow_msg = self.handle_switch_openflow_msg
        f.add_switchConnection = self.add_switchConnection
        f.remove_switchConnection = self.remove_switchConnection
        return f

    def ofmsg_generator(self, type, xid = 0, data=''):
        if xid == 0:
            xid = random.randint(2, 65534000)
        version = 4
        length = 8 + len(data)
        msg = struct.pack(">bbHI", version, type, length, xid)+data
        return msg

    def is_special_packets(self, packet):
        raw_packet_type = packet[54:56]
        ptype = struct.unpack(">BB", raw_packet_type)
        stringtype = "%02x%02x"%ptype
        if stringtype == "88cc":
            return True
        elif stringtype == "0806":
            return True
        return False

    def schedule(self, swiconn, isSpecialPacket):
        if isSpecialPacket:
            conn = self.find_master(swiconn)
            if conn is None:
                logging.error("We get a random one")
                return self.switch_to_controller[swiconn][0]
            return conn
        return self.switch_to_controller[swiconn][0]

    def find_master(self, swiconn):
        if swiconn not in self.switch_to_master:
            return None
        return self.switch_to_master[swiconn]

    def handle_switch_openflow_msg(self, msg, swiconn):
        openflow_header = struct.unpack(">bbHI", msg[:8])
        type = openflow_header[1]
        xid = openflow_header[3]
        length = openflow_header[2]
        logging.debug("Received Switch message: Type %d xid:%d length:%d from switch:%s" % (type, xid, length, swiconn.transport.getPeer()))

        if type == 0:
            for ip in self.CONTROLLER_IPS:
                clientF = self.getOpenFlowClientFactory(swiconn)
                logging.debug("Established a connection to controller")
                reactor.connectTCP(ip, 6633, clientF)
            reply_msg = self.ofmsg_generator(0, xid)
            logging.debug("Fabriate a hello")
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
            #if self.is_special_packets(msg):
                #print "Special Swi %s Controller %s" %(swiconn.transport.getPeer(), conn.transport.getPeer())
            if conn is None:
                logging.error("Not available schedule")
                exit_fwst()
            conn.transport.write(msg)
                # temp = self.switch_to_controller[swiconn]
                # temp[0].transport.write(msg)

        elif type == 25:
            body = struct.unpack(">IIQ", msg[8:])
            role = body[0]
            controller_conn = self.find_controller_given_reply(swiconn, type - 1, xid)
            logging.info("Role %d request %s <==> %s"%(role, swiconn.transport.getPeer(), controller_conn.transport.getPeer()))
            if role == 2:
                self.switch_to_master[swiconn] = controller_conn
                logging.info("Updating Master Controller Information for %s  <==> %s"%(swiconn.transport.getPeer(), controller_conn.transport.getPeer()))
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
        logging.debug("Received Controller message: Type %d xid:%d length:%d from controller %s to switch:%s" % (type, xid, length, controller_conn.transport.getPeer(), swi_conn.transport.getPeer()))
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



s = OpenFlowService()
reactor.listenTCP(6633, s.getOpenFlowServerFactory())
logging.info("Start running server")
reactor.run()







