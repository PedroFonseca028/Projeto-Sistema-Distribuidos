import zmq

context = zmq.Context()

# Servidores publicam aqui (PUB -> XSUB)
sub = context.socket(zmq.XSUB)
sub.bind("tcp://*:5557")

# Bots se inscrevem aqui (SUB -> XPUB)
pub = context.socket(zmq.XPUB)
pub.bind("tcp://*:5558")

print("proxy: servidores publicam em :5557 (XSUB), bots assinam em :5558 (XPUB)", flush=True)
zmq.proxy(sub, pub)

pub.close()
sub.close()
context.close()
