import zmq
import logging

logger = logging.getLogger(__file__)

class DealerRouterHub(object):
    def __init__(self, context, control_address,
                 dealer_selector_func, dealer_key_func):
        self.context = context
        self.dealer_selector_func = dealer_selector_func
        if dealer_selector_func is None:
            self.dealer_selector_func = self.default_dealer_selector
        self.dealer_key_func = dealer_key_func
        self.dealer_sockets = {}
        self.router_socket = self.context.socket(zmq.ROUTER)
        self.control_socket = self.context.socket(zmq.ROUTER)
        self.control_socket.bind(control_address)
        self.poller = zmq.Poller()

        self.poller.register(self.router_socket, zmq.POLLIN)
        self.poller.register(self.control_socket, zmq.POLLIN)

        self.quit = False

        self.actions = {
            "d-connect": self.handle_connect_dealer,
            "r-bind": self.handle_bind_router,
            "ping": self.handle_ping,
            "quit": self.handle_quit_request,
        }

        self.log_ping = False

    def default_dealer_selector(self, hub, frames):
        return self.dealer_sockets.items()

    def run(self):
        while not self.quit:
            for socket, event in self.poller.poll(timeout=1000):
                if socket == self.router_socket:
                    frames = self.router_socket.recv_multipart()
                    # fire to all upstreams - be more selective by inspecting the frames
                    # i.e. have the content provide where to deliver to.
                    dealer_sockets = self.dealer_selector_func(self, frames)
                    for address, dealer_socket in dealer_sockets:
                        dealer_socket.send_multipart(frames)
                    logger.info("< %s", frames)
                    continue

                if socket == self.control_socket:
                    frames = socket.recv_multipart()
                    self.handle_control(socket, frames)
                    continue

                if True: # must be a dealer socket
                    dealer_socket = socket
                    frames = dealer_socket.recv_multipart()
                    self.router_socket.send_multipart(frames)
                    logger.info("> %s", frames)
                    continue


    def handle_control(self, socket, frames):
        if len(frames) < 3:
            logger.warn("Invalid frames: [%s]", frames)
            return

        sender = frames[0]
        action_string = frames[1]

        if action_string == "ping":
            if self.log_ping:
                logger.debug("Control Command: %s", frames)
        else:
            logger.debug("Control Command: %s", frames)

        action = self.actions.get(action_string, None)
        response = ["fail"]
        if action is not None:
            try:
                response = action(socket, sender, frames[2:])
                response = [str(n) for n in response]
            except Exception as ex:
                logger.error(ex)
                response = ["error", str(ex)]
        else:
            logger.error("Unknown action: %s %s", action_string, frames)
            response = ["error", "Unknown Action"]

        socket.send_multipart([sender, action_string] + response)

    def handle_quit_request(self, socket, sender, frames):
        logger.info("quitting")
        self.quit = True
        return True, "ok"

    def handle_ping(self, socket, sender, frames):
        if self.log_ping:
            logger.debug("ping")
        return [True, "pong"] + frames

    def handle_connect_dealer(self, socket, sender, frames):
        address = frames[0]
        key = None
        if self.dealer_key_func is not None:
            key = self.dealer_key_func(frames)
        logger.info("Dealer Connect [%s]", address)
        if address in self.dealer_sockets:
            return True, "duplicate"

        if key is not None:
            if key in self.dealer_sockets:
                return True, "duplicate"

        socket = self.context.socket(zmq.DEALER)
        self.dealer_sockets[address] = socket
        if key is not None:
            self.dealer_sockets[key] = socket

        self.poller.register(socket, zmq.POLLIN)
        socket.connect(address)

        return True, "ok"

    def handle_bind_router(self, socket, sender, frames):
        address = frames[0]
        logger.info("Router Bind [%s]", address)
        self.router_socket.bind(address)
        return True, "ok"


