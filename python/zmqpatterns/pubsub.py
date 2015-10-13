import zmq
import logging

logger = logging.getLogger(__file__)

class PubSubHub(object):
    def __init__(self, context, control_address):
        self.context = context
        # TODO: Maybe another that uses XPUB/XSUB?
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, "")
        self.pub_socket = self.context.socket(zmq.PUB)
        self.control_socket = self.context.socket(zmq.ROUTER)
        self.control_socket.bind(control_address)
        self.poller = zmq.Poller()

        self.poller.register(self.sub_socket, zmq.POLLIN)
        self.poller.register(self.control_socket, zmq.POLLIN)

        self.quit = False

        self.actions = {
            "sub-connect": self.handle_connect_sub,
            "pub-bind": self.handle_bind_pub,
            "ping": self.handle_ping,
            "quit": self.handle_quit_request,
        }

    def run(self):
        while not self.quit:
            for socket, event in self.poller.poll(timeout=1000):
                if socket == self.sub_socket:
                    frames = self.sub_socket.recv_multipart()
                    self.pub_socket.send_multipart(frames)
                    continue

                if socket == self.control_socket:
                    frames = socket.recv_multipart()
                    self.handle_control(socket, frames)
                    continue

    def handle_control(self, socket, frames):
        if len(frames) < 3:
            logger.warn("Invalid frames: [%s]", frames)
            return

        logger.debug("Control Command: %s", frames)

        sender = frames[0]
        action_string = frames[1]

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
        logger.debug("ping")
        return [True, "pong"] + frames

    def handle_connect_sub(self, socket, sender, frames):
        address = frames[0]
        logger.info("SUB Connect [%s]", address)
        self.sub_socket.connect(address)
        return True, "ok"

    def handle_bind_pub(self, socket, sender, frames):
        address = frames[0]
        logger.info("PUB Bind [%s]", address)
        self.pub_socket.bind(address)
        return True, "ok"


