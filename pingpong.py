import logging

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from genpy.pp import PingPongService
from server import TSingleServer

logger = logging.getLogger(__name__)
logging.basicConfig()
logging.root.setLevel(logging.NOTSET)

# wait for ping from thrift client
# save observations
# call train on baseline
# return action to step rpc


class PPHandler:

    def __init__(self):
        self.obs = None
        self.action = "init_action"

    def ping(self, obs):
        self.obs = obs
        logger.info("ping obs: %s", self.obs)
        return self.action


N = 10


def step(handler):
    return handler.obs


def train(handler, i):
    action = f"action{i}"
    obs = step(handler)
    handler.action = action


def main():
    handler = PPHandler()
    processor = PingPongService.Processor(handler)
    transport = TSocket.TServerSocket(host='127.0.0.1', port=9090)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    s = TSingleServer(processor, transport, tfactory, pfactory)
    s.start_listening()

    for i in range(N):
        logger.info("train iter: %d", i)
        s.serve()
        train(handler, i)


if __name__ == '__main__':
    main()
