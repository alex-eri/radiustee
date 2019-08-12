import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class ClientDatagramProtocol(asyncio.DatagramProtocol):
    def __init__(self, data, future, *a, **kw):
        self.data = data
        self.future = future
        super().__init__()

    def connection_made(self, transport):
        self.transport = transport
        self.transport.sendto(self.data)
        print(2.1)
        print(self.data[1])

    def datagram_received(self, data, addr):
        self.future.set_result(data)
        print(2.2)
        print(data[1])


class ProxyDatagramProtocol(asyncio.DatagramProtocol):

    def __init__(self, remotes, loop):
        self.remotes = remotes
        self.loop = loop
        super().__init__()

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        futures = []
        tps = []  # futures with transports, protocols

        for remote in self.remotes:
            future = asyncio.Future()
            futures.append(future)
            protocol = ClientDatagramProtocol(data, future)

            connect = self.loop.create_datagram_endpoint(
                lambda: protocol,
                remote_addr=remote
                )
            connect = asyncio.ensure_future(connect)
            connect.add_done_callback(lambda f: tps.append(f.result()))

        waiter = asyncio.wait(
            futures,
            loop=self.loop, timeout=10, return_when=asyncio.FIRST_COMPLETED)

        def done(dp, *a, **kw):
            print(dp, *a, **kw)
            d, p = dp.result()
            for f in d:
                self.transport.sendto(f.result(), addr=addr)
                break
            for f in p:
                f.cancel()
            for tp in tps:
                transport, protocol = tp
                transport.close()

        asyncio.ensure_future(waiter).add_done_callback(done)


async def main(portmap, loop):

    servers = []
    for port in portmap.keys():
        remotes = portmap[port]
        servers.append(await loop.create_datagram_endpoint(
            lambda: ProxyDatagramProtocol(remotes, loop),
            local_addr=('0.0.0.0', port))
        )



if __name__ == "__main__":

    portmap = {
        1812: [("127.0.0.1", 1816), ("127.0.0.1", 1818)],
        1813: [("127.0.0.1", 1817), ("127.0.0.1", 1819)]
               }

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(portmap,loop))
    loop.run_forever()

    # python3.7+ asyncio.run(main(portmap))
