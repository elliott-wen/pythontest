from twisted.internet.protocol import Protocol
from twisted.internet.protocol import ClientFactory
from twisted.internet.protocol import ServerFactory

from twisted.internet import reactor

import struct
import logging
import random
logging.basicConfig(level=logging.INFO)

'''
Each scheduler maintains a TCP connection with each controller
This TCP connection is used as the path for all the switches' packets from the scheduler to the controller
In the service/protocol class, we need to define a transaction protocol for the communication between the scheduler and the controllers
The protocol is used to pack all the packets from the switches with their datapath ID and
unpack the packets from the controller with the switch's datapath ID

In the controller part, we have an application which is in charge of the packets sending from the TCP connection of the scheduler
The application should implement the packing process and the unpacking process for the controller with the switch datapath ID

Protocol:
struct header {
    unit64_t datapath_ID;               /* Datapath ID associated with this packet. */
    unit16_t length;                    /* Length not including this header. */
}

Initialization: Datapath_ID for the messages such as "HELLO" is 0
before the scheduler receives the switch's datapath_ID
The controller is associated with the transaction ID of the packet
'''


'''################ Handling Connections between Switches and Schedulers ###################'''


class OpenFlowProxyServerProtocol(Protocol):

    def __init__(self):
        self.pending_bytes = 0
        self.data_buffer = bytearray()

    def connectionMade(self):
        self.factory.add_switchConnection(self)
        logging.info("Receiving a switch connection")

    def connectionLost(self, reason):
        self.factory.remove_switchConnection(self)
        #self.transport.setTcpNoDelay(True)
        logging.info("Losing a switch connection!")

    def dataReceived(self, data):
        self.data_buffer.extend(data)
        # logging.debug("Receiving new data from switch %d, buffer data %d"%(len(data), len(self.data_buffer)))
        while True:
            available_bytes = len(self.data_buffer)
            if self.pending_bytes == 0:
                if available_bytes < 8:
                    # logging.debug("Waiting Header")
                    return
                parsed_header = struct.unpack(">bbHI", self.data_buffer[:8])
                # logging.debug("Switch Msg Version:%d Type:%d Length:%d ID:%d" % parsed_header)
                self.pending_bytes = parsed_header[2]

            if self.pending_bytes > available_bytes:
                # logging.debug("Waiting switch entire message %d" % self.pending_bytes)
                return
            openflow_msg = self.data_buffer[:self.pending_bytes]
            self.data_buffer = self.data_buffer[self.pending_bytes:]
            self.factory.handle_switch_openflow_msg(openflow_msg, self)
            self.pending_bytes = 0


'''################## Handle connections between scheduler and ONOS tunnels ##########################'''


class OpenFlowClientProtocol(Protocol):

    def __init__(self):
        self.pending_bytes = 0
        self.data_buffer = bytearray()

    def dataReceived(self, data):
        self.data_buffer.extend(data)
        # logging.debug("Receiving new data from controller %d, buffer data %d"%(len(data), len(self.data_buffer)))
        while True:
            available_bytes = len(self.data_buffer)
            if self.pending_bytes == 0:
                if available_bytes < 10:
                    # logging.debug("Waiting Header")
                    return
                parsed_header = struct.unpack(">QH", self.data_buffer[:10])
                #logging.debug("Tunnel Msg DPID:%d length:%d" % parsed_header)
                self.pending_bytes = parsed_header[1]+10
            if self.pending_bytes > available_bytes:
                # logging.debug("Waiting controller entire message %d" % self.pending_bytes)
                return
            openflow_msg = self.data_buffer[:self.pending_bytes]
            self.data_buffer = self.data_buffer[self.pending_bytes:]
            self.factory.handle_tunnel_openflow_msg(openflow_msg, self)
            self.pending_bytes = 0

    def connectionMade(self):
        logging.info("Connecting to a tunnel!")
        self.transport.setTcpNoDelay(True)
        self.factory.add_tunnelConnection(self)
        # try:
        #     self.transport.setTcpKeepAlive(1)
        # except AttributeError:
        #     logging.debug("Fail to setTcpKeepAlive")
        # type = 0
        # xid = random.randint(2,65534)
        # version = 4
        # length = 8
        # reply_msg = struct.pack(">bbHI", version, type, length, xid)
        # self.transport.write(reply_msg)

    def connectionLost(self, reason):
        logging.info("Losing a tunnel!")
        self.factory.remove_tunnelConnection(self)


'''######################### Service ################################'''


class OpenFlowService():

    switches = []
    tunnels = []
    dpid2sw_dict = {}     # dpid2sw_dict = { Datapath ID: switch_connection }
    sw2dpid_dict = {}     # sw2dpid_dict = { switch_connection: Datapath ID }
    xid2tun = {}          # xid2tun = { transaction ID: tunnel }
    rr = 0                # round-robin factor
    SWITCH_REPLY_TYPES = [3, 8, 19, 21, 25, 27]  # messages from switches to tunnel
    TUNNEL_IGNORE_TYPES = [13, 14, 15, 16, 17, 28, 29] # messages from tunnel, no reply from switch is required

    def ofmsg_generator(self, type, xid):
        if xid == 0:
            xid = random.randint(2, 65534000)
        version = 4
        length = 8
        msg = struct.pack(">bbHI", version, type, length, xid)
        return msg

    def add_switchConnection(self, conn):
        self.switches.append(conn)

    def remove_switchConnection(self, conn):
        # msg = self.ofmsg_generator(1, 0)  # send an error msg to all the controllers to drop the connection
        # for tunnel in self.tunnels:
        #     tunnel.transport.write(str(msg))
        #     self.switch_to_tunnel(msg, conn, 0, )
        #     logging.info("Brocasting error msg")
        self.switches.remove(conn)
        if conn in self.sw2dpid_dict:
            dpid = self.sw2dpid_dict[conn]
            del self.dpid2sw_dict[dpid]
            del self.sw2dpid_dict[conn]

    def add_tunnelConnection(self, conn):
        self.tunnels.append(conn)

    def remove_tunnelConnection(self, conn):
        self.tunnels.remove(conn)
        if len(self.tunnels) == 0:
            logging.info("No available tunnel anymore")
            reactor.stop()
            exit(1)

    def xid_2_tunnel(self, dpid,  xid, remove=True):
        if dpid in self.xid2tun and xid in self.xid2tun[dpid]:    # REMOVE=TRUE: the packet is a reply packet from the switch
            tunnel = self.xid2tun[dpid][xid]
            if remove:
                del self.xid2tun[dpid][xid]
            return tunnel
        return None

    def write_tunnel(self, dpid, msg, tunnel_conn):
        header = struct.pack(">QH", dpid, len(msg))
        fmsg = header + msg
        tunnel_conn.transport.write(fmsg)
        logging.debug("Writing a msg to tunnel %d"%dpid)

    def write_switch(self, msg, switch_conn):
        switch_conn.transport.write(msg)
        logging.debug("Writing a msg to switch")

    def scheduling(self):
        tunnel_num = len(self.tunnels)
        self.rr = (self.rr+1)%len(self.tunnels)
        logging.info("Packet_in msg: switch==>tunnel%d" % self.rr)
        return self.tunnels[self.rr]

    def handle_tunnel_openflow_msg(self, msg, conn):
        header = struct.unpack(">QH", msg[:10])
        dpid = header[0]
        msg = msg[10:]
        openflow_header = struct.unpack(">bbHI", msg[:8])
        type = openflow_header[1]
        xid = openflow_header[3]
        logging.debug("Received a tunnel msg type:%d xid:%s" % (type, xid))

        if dpid not in self.dpid2sw_dict:   # all the messages sent to the tunnels should has its own dpid
            logging.error("Unexpected Error in dpid to switch")
            exit(1)
        swi = self.dpid2sw_dict[dpid]
        self.write_switch(str(msg), swi)
        if type not in self.TUNNEL_IGNORE_TYPES:  # write down the xid for the message because reply from switch is needed
            if dpid not in self.xid2tun:
                self.xid2tun[dpid] = {}
            self.xid2tun[dpid][xid] = conn
            # logging.debug("Saving %d xid %d dpid"%(xid, dpid))
        # else:
        #     logging.debug("No need to save")

    def handle_switch_openflow_msg(self, msg, conn):
        openflow_header = struct.unpack(">bbHI", msg[:8])
        type = openflow_header[1]
        xid = openflow_header[3]
        length = openflow_header[2]
        logging.debug("Received Switch message: Type %d xid:%d length:%d"%(type,xid,length))

        if len(self.tunnels) == 0:
            logging.error("No tunnels")
            reactor.stop()
            exit(1)

        # scheduler sends HELLO(0) to switch and then sends FEATURE_REQUEST(5) to get switch datapath ID
        if type == 0:
            self.write_switch(str(msg), conn)
            logging.debug("Scheduler sends hello message to switch")
            # scheduler sends a OFPT_REATURES_REQUEST(5) to switch to get its DPID
            msg = self.ofmsg_generator(5, 0)
            self.write_switch(str(msg), conn)
            logging.debug("Scheduler sends feature_request to switch to obtain its DPID")

        # When ECHO_REQUEST(2) is received from switch, scheduler sends ECHO_REPLY(3) back to switch
        elif type == 2:
            reply_msg = self.ofmsg_generator(3, xid)
            self.write_switch(str(reply_msg), conn)
            logging.debug("Scheduler sends echo_reply to switch")

        # Different actions are taken which depend on whether the scheduler or the tunnel sends the FEATURE_REQUEST(5)
        elif type == 6:
            # logging.debug("Switch sends feature_reply to scheduler")
            dpid = struct.unpack(">Q", msg[8:16])[0]
            logging.debug("Got a DPID:%d" % dpid)
            if dpid not in self.dpid2sw_dict:      # FEATURE_REQUEST(5) comes from scheduler, no need to reply to tunnel
                self.dpid2sw_dict[dpid] = conn
                self.sw2dpid_dict[conn] = dpid
                logging.debug("Scheduler obtain switch DPID:%s" % dpid)
                lmsg = self.ofmsg_generator(0, 0)   # scheduler sends Hello to all tunnels
                for tunnel in self.tunnels:
                    self.write_tunnel(dpid, lmsg, tunnel)
                    logging.info("Switch==>Tunnel: Hello")
            else:   # FEATURE_REQUEST(5) comes from tunnel, reply to tunnel according to its xid
                tunnel = self.xid_2_tunnel(dpid, xid)
                if tunnel is None:
                    logging.error("Tunnel Not supposed to be None when sending type 6")
                    exit(1)
                self.write_tunnel(dpid, str(msg), tunnel)

        elif type == 19:   # Equal or more than one MULTIPART_REPLY(19) are sent from the switch
            dpid = self.sw2dpid_dict[conn]
            tunnel = self.xid_2_tunnel(dpid, xid, False)
            if tunnel is None:
                logging.error("Tunnel Not Found! ")
                exit(1)
            self.write_tunnel(dpid, str(msg), tunnel)

        elif type == 10:  # Packet_in(10) message should be sent to the tunnel according to the scheduling algorithm
            logging.debug("Switch sends packet_in to scheduler")
            tunnel = self.scheduling()
            dpid = self.sw2dpid_dict[conn]
            self.write_tunnel(dpid, str(msg), tunnel)

        else:
            dpid = self.sw2dpid_dict[conn]
            if type in self.SWITCH_REPLY_TYPES:        # Sent back the reply message to the tunnel according to its xid and delete the item from the dictionary
                tunnel = self.xid_2_tunnel(dpid, xid)
                if tunnel is None:
                    logging.error("Tunnel Not Found! ")
                    exit(1)
                self.write_tunnel(dpid, str(msg), tunnel)
            else:
                logging.info("Broadcasting!!! Type: %d" % type)   # The message is initialized by switch and is sent to all tunnels
                for tunnel1 in self.tunnels:
                    self.write_tunnel(dpid, str(msg), tunnel1)

    def getOpenFlowServerFactory(self):
        f = ServerFactory()
        f.protocol = OpenFlowProxyServerProtocol
        f.handle_switch_openflow_msg = self.handle_switch_openflow_msg
        f.add_switchConnection = self.add_switchConnection
        f.remove_switchConnection = self.remove_switchConnection
        return f

    def getOpenFlowClientFactory(self):
        f = ClientFactory()
        f.protocol = OpenFlowClientProtocol
        f.add_tunnelConnection = self.add_tunnelConnection
        f.handle_tunnel_openflow_msg = self.handle_tunnel_openflow_msg
        f.remove_tunnelConnection = self.remove_tunnelConnection
        return f


tunnel_IPS= ["10.0.3.7"]
s = OpenFlowService()
reactor.listenTCP(6633, s.getOpenFlowServerFactory())
clientF = s.getOpenFlowClientFactory()
for ip in tunnel_IPS:
    reactor.connectTCP(ip, 9999, clientF)
logging.info("Start running server")
reactor.run()












'''#######################################################################################'''

'''proxy.py'''



from twisted.internet.protocol import Protocol
from twisted.internet.protocol import ClientFactory
from twisted.internet.protocol import ServerFactory

from twisted.internet import reactor

import struct
import logging
import random

logging.basicConfig(level=logging.DEBUG)


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
            print "Unable to remove dpid non-exist"

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




