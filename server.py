import logging

from thrift.protocol.THeaderProtocol import THeaderProtocolFactory
from thrift.server.TServer import TServer
from thrift.transport import TTransport

logger = logging.getLogger(__name__)


class TSingleServer(TServer):
    """Simple single-threaded server that serves one request at a time."""

    def __init__(self, *args):
        TServer.__init__(self, *args)
        self.client = None
        self.iprot = None
        self.oprot = None

    def serve(self):
        self.serverTransport.listen()

        if not self.client:
            self.client = self.serverTransport.accept()

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
