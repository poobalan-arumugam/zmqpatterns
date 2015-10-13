import zmq


def test():
    context = zmq.Context()
    dealer1 = context.socket(zmq.DEALER)
    dealer2 = context.socket(zmq.DEALER)

    dealer1.bind("tcp://127.0.0.1:9942")
    dealer2.connect("tcp://127.0.0.1:9942")

    send_frames = ["Hello", "World"]
    dealer1.send_multipart(send_frames)
    recved_frames = dealer2.recv_multipart()

    assert send_frames == recved_frames, (send_frames,
                                          recved_frames)


test()

