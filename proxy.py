from twisted.internet.protocol import Protocol
from twisted.internet.protocol import ClientFactory
from twisted.internet.protocol import ServerFactory

from twisted.internet import reactor

import struct
import logging
import random


logging.basicConfig(level=logging.INFO)


# Handling Connections between Switches and Tunnels
class OpenFlowTunnelProtocol(Protocol):

    def __init__(self):
        self.pending_bytes = 0
        self.data_buffer = bytearray()

    def connectionMade(self):
        self.factory.add_tunnelConnection(self)
        self.transport.setTcpNoDelay(True)
        logging.info("Receiving a tunnel connection")

    def connectionLost(self, reason):
        self.factory.remove_tunnelConnection(self)
        logging.info("Losing a tunnel connection")

    def dataReceived(self, data):
        self.data_buffer.extend(data)
        # logging.debug("Receiving new data from switch %d, buffer data %d" % (len(data), len(self.data_buffer)))
        while True:
            available_bytes = len(self.data_buffer)
            if self.pending_bytes == 0:
                if available_bytes < 10:
                    # logging.debug("Waiting Header")
                    return
                parsed_header = struct.unpack(">QH", self.data_buffer[:10])
                #logging.debug("DPID:%s length:%s" % parsed_header)
                self.pending_bytes = parsed_header[1] + 10

            if self.pending_bytes > available_bytes:
                # logging.debug("Waiting switch entire message %d" % self.pending_bytes)
                return
            openflow_msg = self.data_buffer[:self.pending_bytes]
            self.data_buffer = self.data_buffer[self.pending_bytes:]
            self.factory.forward_openflow_msg(openflow_msg, self)
            self.pending_bytes = 0


# Handle Connections between Tunnels and Controllers
class OpenFlowClientProtocol(Protocol):

    def __init__(self):
        self.pending_bytes = 0
        self.data_buffer = bytearray()

    def dataReceived(self, data):
        self.data_buffer.extend(data)
        # logging.d
        # ebug("Receiving new data from controller %d, buffer data %d"%(len(data), len(self.data_buffer)))
        while True:
            available_bytes = len(self.data_buffer)
            if self.pending_bytes == 0:
                if available_bytes < 8:
                    #logging.debug("Waiting Header")
                    return
                parsed_header = struct.unpack(">bbHI", self.data_buffer[:8])
                #logging.debug("Controller Msg Version:%d Type:%d Length:%d ID:%d" % parsed_header)
                self.pending_bytes = parsed_header[2]

            if self.pending_bytes > available_bytes:
                #logging.debug("Waiting controller entire message %d" % self.pending_bytes)
                return
            openflow_msg = self.data_buffer[:self.pending_bytes]
            self.data_buffer = self.data_buffer[self.pending_bytes:]
            self.factory.handle_controller_openflow_msg(openflow_msg, self)
            self.pending_bytes = 0

    def connectionMade(self):
        logging.info("Connecting to a controller!")
        self.transport.setTcpNoDelay(True)
        self.factory.add_controllerConnection(self, self.factory.dpid)
        type = 0
        xid = random.randint(2,65534)
        version = 4
        length = 8
        reply_msg = struct.pack(">bbHI", version, type, length, xid)
        self.transport.write(reply_msg)

    def connectionLost(self, reason):
        logging.info("Losting a controller!")
        self.factory.remove_controllerConnection(self, self.factory.dpid)



class OpenFlowService():
    tunnel = None

    dpid_controller = {}
    controller_dpid = {}

    def add_tunnelConnection(self, conn):
        self.tunnel = conn

    def remove_tunnelConnection(self, conn):
        self.tunnel = None
        logging.info("Shutting down the tunnel!!!")
        reactor.stop()

    def add_controllerConnection(self, conn, dpid):
        self.dpid_controller[dpid] = conn
        self.controller_dpid[conn] = dpid

    def remove_controllerConnection(self, conn, dpid):
        if dpid in self.dpid_controller:
            print "Losing a connection, removing conn and dpid"
            conn = self.dpid_controller[dpid]
            del self.dpid_controller[dpid]
            del self.controller_dpid[conn]
        else:
            logging.debug("Unable to remove dpid non-exist")

    def handle_controller_openflow_msg(self, msg, conn):
        openflow_header = struct.unpack(">bbHI", msg[:8])
        # logging.debug("Controller Msg Version:%d Type:%d Length:%d ID:%d" % openflow_header)
        type = openflow_header[1]
        xid = openflow_header[3]
        version = openflow_header[0]
        length = openflow_header[2]
        if type == 0:
            logging.debug("It is a hello from controller")
        elif type == 3:
            logging.debug("Echo reply from controller")
        elif type == 24:
            logging.debug("length============%d %d" % (len(msg), len(msg[8:])))
            body = struct.unpack(">IIQ", msg[8:])
            role = body[0]
            padding = body[1]
            generation_id = body[2]
            if role == 0:
                role_msg = "No change"
            elif role == 1:
                role_msg = "Equal"
            elif role == 2:
                role_msg = "Master"
            elif role == 3:
                role_msg = "Slave"
            logging.info("Role_request: change role to %s generation_id: %d DPID: %d==================================" % (role_msg, generation_id, self.controller_dpid[conn]))
            # role = 2
            role_reply = struct.pack(">bbHIIIQ", version, type+1, length, xid, role, padding, generation_id)
            conn.transport.write(str(role_reply))
            logging.debug("Role_reply: change role to Master")

        elif conn in self.dpid_controller.values():
            dpid = self.controller_dpid[conn]
            header = struct.pack(">QH", dpid, length)
            msg = header+msg
            logging.debug("Controller===>Tunnel, type:%s, dpid:%d, xid:%d" % (type, dpid, xid))
            self.tunnel.transport.write(str(msg))
        else:
            logging.error("Unexpected Error for handling controller msg: no dpid")

    def forward_openflow_msg(self, msg, conn):
        #print "Len %d"%(len(msg))
        openflow_header = struct.unpack(">QHbbHI", msg[:18])
        dpid = openflow_header[0]
        type = openflow_header[3]
        # logging.debug("Tunnel Msg Version: Type:%d DPID:%d" % (type, dpid))
        if dpid not in self.dpid_controller:
            if type == 0:
                clientF = self.getOpenFlowClientFactory(dpid)
                logging.debug("Established a connection to controller")
                reactor.connectTCP("localhost", 6633, clientF)
            else:
                logging.error("Protocol error!!!!! Type:%s dpid:%s xid:%d"%(type,dpid,openflow_header[5]))
        elif dpid in self.dpid_controller:
            logging.debug("Tunnel===>Controller, type:%s, dpid:%d"%( type, dpid))
            self.dpid_controller[dpid].transport.write(str(msg[10:]))


    def getOpenFlowServerFactory(self):
        f = ServerFactory()
        f.protocol = OpenFlowTunnelProtocol
        f.forward_openflow_msg = self.forward_openflow_msg
        f.add_tunnelConnection = self.add_tunnelConnection
        f.remove_tunnelConnection = self.remove_tunnelConnection
        return f

    def getOpenFlowClientFactory(self,dpid):
        f = ClientFactory()
        f.protocol = OpenFlowClientProtocol
        f.dpid = dpid
        f.add_controllerConnection = self.add_controllerConnection
        f.handle_controller_openflow_msg = self.handle_controller_openflow_msg
        f.remove_controllerConnection = self.remove_controllerConnection
        return f



s = OpenFlowService()
reactor.listenTCP(9999, s.getOpenFlowServerFactory())
logging.info("Start running server")
reactor.run()