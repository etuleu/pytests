import selectors
import socket

import logging

from thrift.protocol.THeaderProtocol import THeaderProtocolFactory
from thrift.server.TServer import TServer
from thrift.transport import TTransport
from thrift.transport.TSocket import TSocket


logger = logging.getLogger(__name__)


class TSingleServer(TServer):
    """Simple single-threaded server that serves one request at a time."""

    def __init__(self, *args):
        TServer.__init__(self, *args)
        self.client = None
        self.iprot = None
        self.oprot = None
        self.lsock = None
        self.selector = selectors.DefaultSelector()

    def start_listening(self):
        self.lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        host, port = self.serverTransport.host, self.serverTransport.port
        self.lsock.bind((host, port))
        self.lsock.setblocking(False)
        self.lsock.listen()
        logger.info(f"Listening on {(host, port)}")
        self.selector.register(self.lsock, selectors.EVENT_READ, data=None)

    def listen(self):
        events = self.selector.select(timeout=None)
        for key, mask in events:
            if key.data is None:
                client_sock, addr = self.lsock.accept()
                self.client = TSocket()
                self.client.setHandle(client_sock)

    def serve(self):
        if not self.lsock:
            raise Exception("call start_listening first please")

        if not self.client:
            logger.info("waiting for a client to connect")
            self.listen()

            self.itrans = self.inputTransportFactory.getTransport(self.client)
            self.iprot = self.inputProtocolFactory.getProtocol(self.itrans)

            # for THeaderProtocol, we must use the same protocol instance for
            # input and output so that the response is in the same dialect that
            # the server detected the request was in.
            if isinstance(self.inputProtocolFactory, THeaderProtocolFactory):
                self.otrans = None
                self.oprot = self.iprot
            else:
                self.otrans = self.outputTransportFactory.getTransport(self.client)
                self.oprot = self.outputProtocolFactory.getProtocol(self.otrans)

        try:
            self.processor.process(self.iprot, self.oprot)
        except TTransport.TTransportException:
            pass
        except Exception as x:
            logger.exception(x)
