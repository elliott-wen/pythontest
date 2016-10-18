from twisted.internet.protocol import Protocol
from twisted.internet.protocol import ClientFactory
from twisted.internet.protocol import ServerFactory

from twisted.internet import reactor

import struct
import logging
import random
logging.basicConfig(level=logging.DEBUG)

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
    Switch_NORMAL_TYPES = [1, 3, 4, 8, 11, 12, 19, 21, 25, 27]  # messages from switches to tunnel
    TUNNEL_IGNORE_TYPES = [13, 14, 15, 16, 17, 28, 29] # messages from tunnel, no reply from switch is required

    def ofmsg_generator(self,type,xid):
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

    # Pack switch's messages with its DPID
    def pack_msg(self, msg, conn):
        if conn in self.sw2dpid_dict:
            dpid = self.sw2dpid_dict[conn]
        else:
            dpid = 0
        openflow_header = struct.unpack(">bbHI", msg[:8])
        length = openflow_header[2]
        # logging.debug("Pack Switch Msg DPID:%s length:%s"%(dpid, length))
        header = struct.pack(">QH", dpid, length)
        msg = header+msg
        return msg


    def xid_2_tunnel(self, xid):
        if xid in self.xid2tun:    # whether the packet is a reply packet to the controller
            tunnel = self.xid2tun[xid]
            del self.xid2tun[xid]
            return tunnel
        return None



    def write_tunnel(self, dpid, msg, tunnel_conn):
        # openflow_header = struct.unpack(">bbHI", msg[:8])
        # print "Len:%d %d"%(openflow_header[2], len(msg))
        header = struct.pack(">QH", dpid, len(msg))
        fmsg = header + msg
        tunnel_conn.transport.write(fmsg)
        logging.debug("Writing a msg to tunnel %d"%dpid)


    def write_switch(self, msg, switch_conn):
        switch_conn.transport.write(msg)
        logging.debug("Writing a msg to switch")


    def switch_to_tunnel(self, msg, conn, xid, type):
        msg = self.pack_msg(msg, conn)
        # !!!if it is feature_reply from the switch, consider multiply replies would be received
        if xid in self.xid2tun:    # whether the packet is a reply packet to the controller
            tunnel = self.xid2tun[xid]
            del self.xid2tun[xid]
            logging.error("Delete XID%d"%xid)
            tunnel.transport.write(str(msg))
            logging.debug("Scheduler message type:%d xid:%s" % (type, xid))
        else:
            for tunnel in self.tunnels:
                tunnel.transport.write(str(msg))
                logging.debug("Brocasting Scheduler message type:%d xid:%s ..............................................." % (type, xid))

    def tunnel_to_switch(self, msg, dpid, type):
        # if dpid == 0:
        #     if len(self.switches) == 0:
        #         logging.debug("Switches = 0")
        #         return
        #     for switch in self.switches:
        #         switch.transport.write(str(msg))
        #     logging.debug("Tunnel message type:%d " % type)
        # else:
            if dpid not in self.dpid2sw_dict:
                logging.debug("DPID not found")
                return
            switch = self.dpid2sw_dict[dpid]
            switch.transport.write(str(msg))
            logging.debug("Tunnel sends message type:%d to switch"%type)

    def scheduling(self):
        tunnel_num = len(self.tunnels)
        r = random.randint(0, tunnel_num-1)
        return self.tunnels[r]

    def handle_tunnel_openflow_msg(self, msg, conn):
        header = struct.unpack(">QH", msg[:10])
        dpid = header[0]
        msg = msg[10:]
        openflow_header = struct.unpack(">bbHI", msg[:8])
        type = openflow_header[1]
        xid = openflow_header[3]
        logging.debug("Received a tunnel msg type:%d xid:%s" % (type, xid))

        # if dpid == 0:
        #     if type == 0:
        #         logging.debug("tunnel sends hello message to scheduler")
        #     elif type == 5:
        #         logging.debug("tunnel sends feature_request to scheduler")
        #         self.tunnel_to_switch(msg, dpid, type)
        #         self.xid2tun[xid] = conn
        #
        # else:
        if type not in self.TUNNEL_IGNORE_TYPES:  # handle tunnel command message, no reply from switch
            self.xid2tun[xid] = conn
            logging.error("Saving XID:%d"%xid)
        if dpid not in self.dpid2sw_dict:
            logging.error("Not dpid in dpid2sw")
            exit(1)
        switch = self.dpid2sw_dict[dpid]
        self.write_switch(str(msg), switch)
        #self.tunnel_to_switch(msg, dpid, type)

    def handle_switch_openflow_msg(self, msg, conn):
        openflow_header = struct.unpack(">bbHI", msg[:8])
        type = openflow_header[1]
        xid = openflow_header[3]
        version = openflow_header[0]
        length = openflow_header[2]
        # logging.debug("Switch message: Type %d"%type)

        # scheduler sends "HELLO" message to all tunnels and then sends Feature_Request to get the datapath ID
        if type == 0:
            logging.debug("Switch sends hello message to scheduler")
            if len(self.tunnels) == 0:
                logging.info("No tunnels")
                return
            #self.switch_to_tunnel(msg, conn, xid, type)
            # scheduler represents all tunnels and sends "HELLO" back to the switch
            self.write_switch(str(msg), conn)
            logging.debug("Scheduler sends hello message to switch")
            # scheduler sends a OFPT_REATURES_REQUEST to switch to get its DPID
            msg = self.ofmsg_generator(5, 0)
            self.write_switch(str(msg), conn)
            logging.debug("Scheduler sends feature_request to switch to obtain its DPID")

        elif type == 2:
            logging.debug("Switch sends echo_request to scheduler")
            self.switch_to_tunnel(msg, conn, xid, type)
            reply_msg = struct.pack(">bbHI", version, 3, length, xid)
            # scheduler sends echo_reply to switch
            self.write_switch(str(reply_msg), conn)
            logging.debug("Scheduler sends echo_reply to switch")

        elif type == 6:
            logging.debug("Switch sends feature_reply to scheduler")
            dpid = struct.unpack(">Q", msg[8:16])[0]
            logging.debug("DPID:%d" % dpid)
            if dpid not in self.dpid2sw_dict:
                self.dpid2sw_dict[dpid] = conn
                self.sw2dpid_dict[conn] = dpid
                logging.debug("Scheduler obtain switch DPID:%s" % dpid)
                lmsg = self.ofmsg_generator( 0, 0)
                self.switch_to_tunnel(lmsg, conn, xid, type)
            else:
                self.switch_to_tunnel(msg, conn, xid, type)
            # dpid = struct.unpack(">Q", msg[8:16])[0]
            # logging.debug("Got a DPID:%d" % dpid)
            # if dpid not in self.dpid2sw_dict:
            #     self.dpid2sw_dict[dpid] = conn
            #     self.sw2dpid_dict[conn] = dpid
            #     logging.debug("Scheduler obtain switch DPID:%s" % dpid)
            #     lmsg = self.ofmsg_generator(0, 0)
            #     for tunnel in self.tunnels:
            #         self.write_tunnel(dpid, str(lmsg), tunnel)
            # else:
            #     tunnel = self.xid_2_tunnel(xid)
            #     if tunnel is None:
            #         logging.error("Tunnel Not supposed to be None when sending type 6")
            #         exit(1)
            #     self.write_tunnel(dpid, str(msg), tunnel)

        elif type == 10:
            # logging.debug("Switch sends packet_in to scheduler")
            logging.debug("Switch sends packet_in to scheduler")
            tunnel = self.scheduling()
            dpid = self.sw2dpid_dict[conn]
            self.write_tunnel(dpid, str(msg), tunnel)

        elif type in self.Switch_NORMAL_TYPES:
            # logging.debug("Switch sends type%d to scheduler" % type)
            self.switch_to_tunnel(msg, conn, xid, type)

        else:
            logging.error("Switch sends type %d message" % type)
            self.switch_to_tunnel(msg, conn, xid, type)

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
