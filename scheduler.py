from twisted.internet.protocol import Protocol
from twisted.internet.protocol import ClientFactory
from twisted.internet.protocol import ServerFactory

from twisted.internet import reactor

import struct
import logging
import random

logging.basicConfig(level=logging.DEBUG)

#Handling Connections between Switches and Schedulers
class OpenFlowServerProtocol(Protocol):

    def __init__(self):
        self.pending_bytes = 0
        self.data_buffer = bytearray()

    def connectionMade(self):
        self.factory.add_switchConnection(self)
        logging.info("Receiving a switch connection")

    def connectionLost(self, reason):
        self.factory.remove_switchConnection(self)
        logging.info("Losing a switch connection")


    def dataReceived(self, data):
        self.data_buffer.extend(data)
        logging.debug("Receiving new data from switch %d, buffer data %d"%(len(data), len(self.data_buffer)))
        while True:
            available_bytes = len(self.data_buffer)
            if self.pending_bytes == 0:
                if available_bytes < 8:
                    #logging.debug("Waiting Header")
                    return
                parsed_header = struct.unpack(">bbHI", self.data_buffer[:8])
                logging.debug("Switch Msg Version:%d Type:%d Length:%d ID:%d"%parsed_header)
                self.pending_bytes = parsed_header[2]

            if self.pending_bytes > available_bytes:
                logging.debug("Waiting switch entire message %d"%self.pending_bytes)
                return
            openflow_msg = self.data_buffer[:self.pending_bytes]
            self.data_buffer = self.data_buffer[self.pending_bytes:]
            self.factory.handle_switch_openflow_msg(openflow_msg, self)                  # handle_switch_openflow_msg instead of handle_controller_openflow_msg
            self.pending_bytes = 0;


#handle connections scheduler and onos controllers
class OpenFlowClientProtocol(Protocol):

    def __init__(self):
        self.pending_bytes = 0
        self.data_buffer = bytearray()

    def dataReceived(self, data):
        self.data_buffer.extend(data)
        logging.debug("Receiving new data from controller %d, buffer data %d"%(len(data), len(self.data_buffer)))
        while True:
            available_bytes = len(self.data_buffer)
            if self.pending_bytes == 0:
                if available_bytes < 8:
                    #logging.debug("Waiting Header")
                    return
                parsed_header = struct.unpack(">bbHI", self.data_buffer[:8])
                logging.debug("Controller Msg Version:%d Type:%d Length:%d ID:%d"%parsed_header)
                self.pending_bytes = parsed_header[2]

            if self.pending_bytes > available_bytes:
                logging.debug("Waiting controller entire message %d"%self.pending_bytes)
                return
            openflow_msg = self.data_buffer[:self.pending_bytes]
            self.data_buffer = self.data_buffer[self.pending_bytes:]
            self.factory.handle_controller_openflow_msg(openflow_msg, self)
            self.pending_bytes = 0;

    def connectionMade(self):
        logging.info("Connecting to a controller! Sending a hello")
        self.factory.add_controllerConnection(self)
        type = 0
        xid = random.randint(2,65534)
        version = 4
        length = 8
        reply_msg = struct.pack(">bbHI", version, type, length, xid)
        self.transport.write(reply_msg)

    def connectionLost(self, reason):
        logging.info("Losing a controller!")
        self.factory.remove_controllerConnection(self)



class OpenFlowService():
    switches = []
    controllers = []

    def add_switchConnection(self, conn):
        self.switches.append(conn)

    def remove_switchConnection(self, conn):
        self.switches.remove(conn)

    def add_controllerConnection(self, conn):
        self.controllers.append(conn)

    def remove_controllerConnection(self, conn):
        self.controllers.remove(conn)

    def handle_controller_openflow_msg(self, msg, conn):
        openflow_header = struct.unpack(">bbHI", msg[:8])
        type = openflow_header[1]
        xid = openflow_header[3]
        version = openflow_header[0]
        length = openflow_header[2]
        if type ==0:                                                                                   # type = 0 instead of 1
            logging.info("It is a hello from controller")
        elif type == 2:
            logging.debug("It is an echo request from controller")
            reply_msg = struct.pack(">bbHI", version, 3, length, xid)
            conn.transport.write(reply_msg)
        elif type == 5:
            logging.debug("It is a feature request from controller")



    def handle_switch_openflow_msg(self, msg, conn):
        openflow_header = struct.unpack(">bbHI", msg[:8])
        type = openflow_header[1]
        xid = openflow_header[3]
        version = openflow_header[0]
        length = openflow_header[2]

        if type == 0:
            logging.debug("It is a hello")
            conn.transport.write(str(msg))
        elif type == 2:
            logging.debug("It is an echo request from switch")
            reply_msg = struct.pack(">bbHI", version, 3, length, xid)
            conn.transport.write(reply_msg)


    def getOpenFlowServerFactory(self):
        f = ServerFactory()
        f.protocol = OpenFlowServerProtocol
        f.handle_switch_openflow_msg = self.handle_switch_openflow_msg
        f.add_switchConnection = self.add_switchConnection
        f.remove_switchConnection = self.remove_switchConnection
        return f

    def getOpenFlowClientFactory(self):
        f = ClientFactory()
        f.protocol = OpenFlowClientProtocol
        f.add_controllerConnection = self.add_controllerConnection
        f.handle_controller_openflow_msg = self.handle_controller_openflow_msg
        f.remove_controllerConnection = self.remove_controllerConnection
        return f


CONTROLLER_IPS= ["10.0.3.7"]
s = OpenFlowService()
reactor.listenTCP(6633, s.getOpenFlowServerFactory())
clientF = s.getOpenFlowClientFactory()
for ip in CONTROLLER_IPS:
    reactor.connectTCP(ip, 6633, clientF)
logging.info("Start running server")
reactor.run()
