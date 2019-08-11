import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class ClientDatagramProtocol(asyncio.DatagramProtocol):
    def init(self, origin, data, event):
        self.data = data
        self.origin = origin
        self.event = event
        super().__init__()

    def connection_made(self, transport):
        self.transport = transport
        self.transport.sendto(self.data)

    def datagram_received(self, data, addr):
        self.transport.sendto(data, addr=self.origin)
        self.event.set()


class ProxyDatagramProtocol(asyncio.DatagramProtocol):

    def __init__(self, remotes, loop):
        self.remotes = remotes
        self.loop = loop
        super().__init__()

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        event = asyncio.Event()

        tps = []

        for remote in self.remotes:
            tps.append(await self.loop.create_datagram_endpoint(
                lambda: ClientDatagramProtocol(addr, data, event),
                remote_addr=remote
                ))

        try:
            self.loop.run_until_complete(asyncio.wait_for(event.wait(), 10))
        except asyncio.TimeoutError:
            pass

        for transport, protocol in tps:
            transport.close()


async def main(portmap):
    loop = asyncio.get_running_loop()
    servers = []
    for port in portmap.keys():
        remotes = portmap[port]
        servers.append(await loop.create_datagram_endpoint(
            lambda: ProxyDatagramProtocol(remotes, loop),
            local_addr=('0.0.0.0', port))
        )

    loop.run_forever()


if __name__ == "__main__":

    portmap = {
        1812: [("121.0.0.1", 1816), ("121.0.0.1", 1818)],
        1813: [("121.0.0.1", 1817), ("121.0.0.1", 1819)]
               }

    asyncio.run(main(portmap))
