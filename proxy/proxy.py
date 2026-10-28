import zmq

context = zmq.Context()

pub = context.socket(zmq.XPUB)
pub.bind("tcp://*:5558")

sub = context.socket(zmq.XSUB)
sub.bind("tcp://*:5557")

print("Proxy iniciado: XSUB tcp://*:5557 (servidores/PUB) <-> XPUB tcp://*:5558 (bots/SUB)", flush=True)

zmq.proxy(pub, sub)

pub.close()
sub.close()
context.close()
