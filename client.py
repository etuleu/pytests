from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from genpy.pp import PingPongService


def main():
    transport = TSocket.TSocket('localhost', 9090)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = PingPongService.Client(protocol)
    transport.open()

    for i in range(10):
        obs = f"obs{i}"
        action = client.ping(obs)
        print('ping action:', action)


if __name__ == '__main__':
    main()
