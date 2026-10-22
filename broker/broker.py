import zmq

context = zmq.Context()

client_socket = context.socket(zmq.ROUTER)
client_socket.bind("tcp://*:5555")

server_socket = context.socket(zmq.DEALER)
server_socket.bind("tcp://*:5556")

print("Broker iniciado: ROUTER tcp://*:5555 (clientes) <-> DEALER tcp://*:5556 (servidores)", flush=True)

zmq.proxy(client_socket, server_socket)

client_socket.close()
server_socket.close()
context.close()
