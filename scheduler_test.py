from twisted.internet.protocol import Protocol
from twisted.internet.protocol import ClientFactory
from twisted.internet.protocol import ServerFactory

from twisted.internet import reactor

import struct
import logging
import random

logging.basicConfig(level=logging.DEBUG)

'''
A demo for one scheduler, one scheduler and two switches
'''

################ Handling Connections between Switches and Schedulers ###################

class OpenFlowServerProtocol(Protocol):

    def __init__(self):
        self.pending_bytes = 0
        self.data_buffer = bytearray()

    def connectionMade(self):
        self.factory.add_switchConnection(self)
        logging.info("Receiving a switch connection")
        try:
            self.transport.setTcpKeepAlive(1)
        except AttributeError:
            logging.debug("Fail to setTcpKeepAlive")

    def connectionLost(self, reason):
        self.factory.remove_switchConnection(self)
        logging.info("Losing a switch connection!")


    def dataReceived(self, data):
        self.data_buffer.extend(data)
        #logging.debug("Receiving new data from switch %d, buffer data %d"%(len(data), len(self.data_buffer)))
        while True:
            available_bytes = len(self.data_buffer)
            if self.pending_bytes == 0:
                if available_bytes < 8:
                    #logging.debug("Waiting Header")
                    return
                parsed_header = struct.unpack(">bbHI", self.data_buffer[:8])
                #logging.debug("Switch Msg Version:%d Type:%d Length:%d ID:%d"%parsed_header)
                self.pending_bytes = parsed_header[2]

            if self.pending_bytes > available_bytes:
                #logging.debug("Waiting switch entire message %d"%self.pending_bytes)
                return
            openflow_msg = self.data_buffer[:self.pending_bytes]
            self.data_buffer = self.data_buffer[self.pending_bytes:]
            self.factory.handle_switch_openflow_msg(openflow_msg, self)                  # handle_switch_openflow_msg instead of handle_controller_openflow_msg
            self.pending_bytes = 0;


################## Handle connections between scheduler and onos controllers ##########################

class OpenFlowClientProtocol(Protocol):

    def __init__(self):
        self.pending_bytes = 0
        self.data_buffer = bytearray()

    def dataReceived(self, data):
        self.data_buffer.extend(data)
        #logging.debug("Receiving new data from controller %d, buffer data %d"%(len(data), len(self.data_buffer)))
        while True:
            available_bytes = len(self.data_buffer)
            if self.pending_bytes == 0:
                if available_bytes < 8:
                    #logging.debug("Waiting Header")
                    return
                parsed_header = struct.unpack(">bbHI", self.data_buffer[:8])
                #logging.debug("Controller Msg Version:%d Type:%d Length:%d ID:%d"%parsed_header)
                self.pending_bytes = parsed_header[2]

            if self.pending_bytes > available_bytes:
                #logging.debug("Waiting controller entire message %d"%self.pending_bytes)
                return
            openflow_msg = self.data_buffer[:self.pending_bytes]
            self.data_buffer = self.data_buffer[self.pending_bytes:]
            self.factory.handle_controller_openflow_msg(openflow_msg, self)
            self.pending_bytes = 0;

    def connectionMade(self):
        logging.info("Connecting to a controller!")
        self.factory.add_controllerConnection(self)
        try:
            self.transport.setTcpKeepAlive(1)
        except AttributeError:
            logging.debug("Fail to setTcpKeepAlive")
        # type = 0
        # xid = random.randint(2,65534)
        # version = 4
        # length = 8
        # reply_msg = struct.pack(">bbHI", version, type, length, xid)
        # self.transport.write(reply_msg)

    def connectionLost(self, reason):
        logging.info("Losing a controller!")
        self.factory.remove_controllerConnection(self)


######################### Service ################################

class OpenFlowService():

    switches = []
    controllers = []
    controller_request = []                                                                                  # new variable to record request from the controller
    reply_type = [3,6,8,19,21,25,27]

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
        for switch in self.switches:
            switch.transport.write(str(msg))
        logging.info("Type %d Controller sends message to switches"%type)

        # if type == 0:
        #     logging.info("Controller sends hello message to scheduler")
        #     for switch in self.switches:
        #         switch.transport.write(str(msg))
        #         logging.debug("Scheduler sends hello to all switches")
        #
        # elif type == 2:
        #     logging.debug("Controller sends echo_request to scheduler")
        #     reply_msg = struct.pack(">bbHI", version, 3, length, xid)
        #     conn.transport.write(reply_msg)
        #     logging.debug("Scheduler sends echo_reply to controller")
        #
        # elif type == 3:
        #     logging.debug("Controller sends echo_reply to scheduler and scheduler drops it")
        #
        # elif type == 5:
        #     logging.debug("Controller sends feature_request to scheduler")
        #     ############# Test ###################
        #     type = 0
        #     xid = random.randint(2, 65534)
        #     version = 4
        #     length = 8
        #     reply_msg = struct.pack(">bbHI", version, type, length, xid)
        #     conn.transport.write(reply_msg)
        #     logging.debug("Switch sends Hello to controller for the 2nd time")
        #     # for switch in self.switches:
        #     #     switch.transport.write(str(msg))
        #     #     logging.debug("Scheduler sends feature_request to all switches")
        #
        # elif type in self.reply_type:
        #     logging.debug("It is a reply from switch")
        # else:
        #     logging.debug("type %d message from controller"%type)
        #     self.controller_request.append(msg)

        #     '''
        #     if len(self.switches) == 0:
        #         logging.debug("There is no switches")
        #         self.feature_msg = msg
        #     else:
        #         logging.debug("There are %d switches"%len(self.switches))
        #         for switch in self.switches:
        #             switch.transport.write(str(msg))
        #             logging.debug("Send a feature request to switches")
        #
        # elif type == 18:
        #     logging.debug("It is a multipart request from controller")
        #     self.multipart_msg = msg
        # else:
        #     logging.debug("type %d message from controller"%type)'''
            ############################################################


    def handle_switch_openflow_msg(self, msg, conn):
        openflow_header = struct.unpack(">bbHI", msg[:8])
        type = openflow_header[1]
        xid = openflow_header[3]
        version = openflow_header[0]
        length = openflow_header[2]
        logging.debug("Switch message: Type %d"%type)

        for controller in self.controllers:
            controller.transport.write(str(msg))
            logging.debug("Type %d Switch sends message to controllers"%type)

        # # scheduler sends "HELLO" message to all controllers to avoid the datapath ID problem
        # if type == 0:
        #     while len(self.controllers) == 0:
        #         return
        #     for controller in self.controllers:
        #         controller.transport.write(str(msg))
        #         logging.debug("Switch sends hello message to all controllers")
        #     # scheduler which represents all controllers send "HELLO" back to the switch
        #     # conn.transport.write(str(msg))
        #     # logging.debug("Scheduler sends hello message to switch")
        #
        # elif type == 2:
        #     logging.debug("Switch sends echo_request to scheduler")
        #     reply_msg = struct.pack(">bbHI", version, 3, length, xid)
        #     #scheduler sends echo_reply to switch
        #     conn.transport.write(reply_msg)
        #     logging.debug("Scheduler sends echo_reply to switch")
        #
        #     # ####################    Add   #########################
        #     # while len(self.controller_request)!=0:
        #     #     conn.transport.write(str(self.controller_request[0]))
        #     #     logging.debug("Scheduler sends a request to switch")
        #     #     del self.controller_request[0]
        #
        # elif type == 3:
        #     logging.debug("Switch sends echo_reply to scheduler and scheduler drops it")
        #
        # elif type == 6:
        #     logging.debug("Switch sends feature_reply to scheduler")
        #
        #
        # else:
        #     logging.debug("It is a type %d message from switch"%type)
        #     for controller in self.controllers:
        #         controller.transport.write(str(msg))
        #
        # '''
        # elif type == 19:
        #     logging.debug("It is a multipart reply from switch")
        #     for controller in self.controllers:
        #         controller.transport.write(str(msg))'''
        ################################################################


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
