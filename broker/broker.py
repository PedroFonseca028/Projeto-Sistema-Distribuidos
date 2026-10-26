import zmq

context = zmq.Context()

frontend = context.socket(zmq.ROUTER)
frontend.bind("tcp://*:5555")

backend = context.socket(zmq.DEALER)
backend.bind("tcp://*:5556")

print("broker: bots em :5555, servidores em :5556", flush=True)
zmq.proxy(frontend, backend)