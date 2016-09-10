from twisted.internet.protocol import Protocol
from twisted.internet.protocol import ClientFactory
from twisted.internet.protocol import ServerFactory

from twisted.internet import reactor

import struct
import logging


logging.basicConfig(level=logging.DEBUG)

#Handling Connections between Switches and Schedulers
class OpenFlowServerProtocol(Protocol):

    def __init__(self):

        self.pending_bytes = 0
        self.data_buffer = bytearray()

    def connectionMade(self):
        self.factory.add_switchConnection(self)
        logging.info("Receving a switch connection")

    def connectionLost(self, reason):
        self.factory.remove_switchConnection(self)
        logging.info("Losing a switch connection")


    def dataReceived(self, data):
        self.data_buffer.extend(data)
        logging.debug("Receving new data %d, buffer data %d"%(len(data), len(self.data_buffer)))
        while True:
            available_bytes = len(self.data_buffer)
            if self.pending_bytes == 0:
                if available_bytes < 8:
                    logging.debug("Waiting Header")
                    return
                parsed_header = struct.unpack(">bbHI", self.data_buffer[:8])
                logging.debug("Version:%d Type:%d Length:%d ID:%d"%parsed_header)
                self.pending_bytes = parsed_header[2]

            if self.pending_bytes > available_bytes:
                logging.debug("Waiting entire message %d"%self.pending_bytes)
                return
            openflow_msg = self.data_buffer[:self.pending_bytes]
            self.data_buffer = self.data_buffer[self.pending_bytes:]
            self.factory.handle_openflow_msg(openflow_msg)
            self.pending_bytes = 0;


#handle connections scheduler and onos controllers
class OpenFlowClientProtocol(Protocol):
    def dataReceived(self, data):
        pass



class OpenFlowService():
    switches = []

    def add_switchConnection(self, conn):
        self.switches.append(conn)


    def remove_switchConnection(self, conn):
        self.switches.remove(conn)

    def clientConnectionLost(self, connector, reason):
        logging.warning("Reconnecting to the server")
        connector.connect()

    def clientConnectionFailed(self, connector, reason):
        logging.info("connection failed:", reason)


    def handle_openflow_msg(self, msg):
        openflow_header = struct.unpack("<bbHI", msg[:8])

        #The type, if the type is hello, or echo.
        #If other, then schedule it

    def getOpenFlowServerFactory(self):
        f = ServerFactory()
        f.protocol = OpenFlowServerProtocol
        f.handle_openflow_msg = self.handle_openflow_msg
        f.add_switchConnection = self.add_switchConnection
        f.remove_switchConnection = self.remove_switchConnection
        return f

    def getOpenFlowClientFactory(self):
        f = ClientFactory()
        f.protocol = OpenFlowClientProtocol
        f.clientConnectionFailed = self.clientConnectionFailed
        f.clientConnectionLost = self.clientConnectionLost
        return f


s = OpenFlowService()
reactor.listenTCP(6633, s.getOpenFlowServerFactory())
logging.info("Start running server")
reactor.run()
