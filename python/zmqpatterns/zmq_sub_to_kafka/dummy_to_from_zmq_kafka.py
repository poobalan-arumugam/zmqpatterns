import zmq
c = zmq.Context()
s = c.socket(zmq.DEALER)
s.connect("tcp://127.0.0.1:10003")

s.send_multipart([b"Hello World1"])
s.send_multipart([b"Hello World2"])
s.recv_multipart()
s.recv_multipart()
s.recv_multipart()
s.recv_multipart()
s.recv_multipart()
