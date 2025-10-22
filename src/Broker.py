import zmq
from uhashring import HashRing
import json
import uuid
import time


HEARTBEAT_INTERVAL = 3  
HEARTBEAT_LIVENESS = 3  # Number of missing heartbeats before considering a worker dead

# Prepare our context and sockets
context = zmq.Context()
frontend = context.socket(zmq.ROUTER)
backend = context.socket(zmq.ROUTER)
frontend.bind("tcp://*:5559")
backend.bind("tcp://*:5560")

# List of backend servers
servers = []
ring = HashRing(servers)
# Initialize poll set
poller = zmq.Poller()
poller.register(frontend, zmq.POLLIN)
poller.register(backend, zmq.POLLIN)

# Dictionary to map server's request to a client
server_to_client = {}
slid_to_server = {}
last_used_server = {}
request_to_client = {}
backend_ready = False
backend_ready = False

'''
Workers[worker] = x
x = 1-3 -> Worker is alive
x = 0 -> Worker is dead
'''
workers = {}
heartbeat_at = time.time() + HEARTBEAT_INTERVAL

def handle_save_delete_request(server, request_id,client_id, request):
    print("broker sending request to server: ", server)
    backend.send_multipart([server.encode(), b"", request_id.encode(), b"", client_id, b"", request])

def handle_get_request(the_three_servers, request_id, client_id, request):
    server1 = the_three_servers[0]
    server2 = the_three_servers[1]
    server3 = the_three_servers[2]
    manage_client_request_on_disconnect([server1, server2, server3], request_id, client_id, request)

def manage_client_request_on_disconnect(servers, request_id, client_id, request):
    for server in servers:
        if workers[server.encode()] == 0:
            if (server == servers[-1]):
                frontend.send_multipart([client_id, b"", b"A server could not be reached"])
            continue
        else:
            server_to_client[server] = client_id
            backend.send_multipart([server.encode(), b"", request_id.encode(), b"", client_id, b"", request])
            break

def initial_server_connections():
    global servers, ring, workers, heartbeat_at
    first_open = True
    while first_open or time.time() < heartbeat_at:
        sockets = dict(poller.poll(timeout=1000))
        try:
            if sockets.get(backend) == zmq.POLLIN:
                request = backend.recv_multipart()
                worker, empty, message = request[:3]
                print("Received message: ", message, " from worker: ", worker)
                if message == b"RECONECT":
                    first_open = False
                    backend.send_multipart([worker, b"", b"Broker", b"", b"READY"])
                elif message == b"NEW_CONNECTION":
                    backend.send_multipart([worker, b"", b"Broker", b"", b"READY"])
                elif message == b"READY":
                    print("Worker connected: ", worker)
                    workers[worker] = HEARTBEAT_LIVENESS
                    servers.append(worker.decode())
                if first_open and len(workers) == 4:
                    break
        except:
            continue
    servers.sort()
    ring = HashRing(servers)
    print("Connected to workers")
    print(workers)

def handle_server_recovery(incoming_worker):
    global servers, ring
    backend.send_multipart([incoming_worker, b"", b"Broker", b"", b"READY"])
    sockets = dict(poller.poll())
    while backend in sockets:
        # Handle worker activity on the backend
        request = backend.recv_multipart()
        worker, empty, message = request[:3]
        if message == b"READY":
            break
    if(incoming_worker in workers):
        print("Server recovered: ", incoming_worker)
        workers[incoming_worker] = HEARTBEAT_LIVENESS
    else:
        print("New server connected: ", incoming_worker)
        servers.append(incoming_worker.decode())
        servers.sort()
        ring = HashRing(servers)
        for worker in ring.get_nodes():
            print(worker)
        nodes = list(ring.get_nodes())
        new_server_index = nodes.index(incoming_worker.decode())
        next_server = nodes[(new_server_index + 1) % len(nodes)]
        second_next_server = nodes[(new_server_index + 2) % len(nodes)]
        third_next_server = nodes[(new_server_index + 3) % len(nodes)]

        workers[incoming_worker] = HEARTBEAT_LIVENESS
        # Send to backend info about the new server
        for worker in list(workers):
            backend.send_multipart([worker, b"",b"",b"", b"Broker", b"", json.dumps({"type": "newServer", "new":incoming_worker.decode() ,"next": next_server, "second": second_next_server, "third": third_next_server}).encode()])

def heartbeat_check():
    global heartbeat_at
    for worker in list(workers):
        if workers[worker] == 0:
            print(f"Worker {worker} disconnected")
        else:
            workers[worker] -= 1
            backend.send_multipart([worker, b"", b"", b"", b"Broker", b"", b"HEARTBEAT"])
        heartbeat_at = time.time() + HEARTBEAT_INTERVAL

def handle_client_request(request):
    client_id = request[0]
    request = request[2]
    # Decode the request and parse the JSON
    request_str = request.decode('utf-8')
    request_dict = json.loads(request_str)
    request_id = str(uuid.uuid4())  # Generate a unique request ID
    print(f"-> [CLIENT] Received {request_dict['type']} request {request_id} from client {client_id}")
    slId = request_dict['slId']
    
    request_to_client[request_id] = client_id

    if (slId not in slid_to_server):
        primary_server = ring.get_node(slId)
        index = servers.index(primary_server)
        the_three_servers = [primary_server,servers[(index + 1) % len(servers)], servers[(index + 2) % len(servers)]]
    else:
        the_three_servers = slid_to_server[slId]

    if request_dict['type'] == "save":
         
        if slId not in last_used_server:
            last_used_server[slId] = 0
        else:
            last_used_server[slId] += 1
            last_used_server[slId] %= 3
        
        # If the server is dead, try the next one
        for _ in range(0,2): 
            if workers[the_three_servers[last_used_server[slId]].encode()] == 0:
                last_used_server[slId] += 1
                last_used_server[slId] %= 3
            else:
                break
        
        server = the_three_servers[last_used_server[slId]]
        server_to_client[server] = client_id
        handle_save_delete_request(server, request_id, client_id, request)
    if request_dict['type'] == "get":
        handle_get_request(the_three_servers, request_id, client_id, request)
    if request_dict['type'] == "delete":
        if slId not in last_used_server:
            last_used_server[slId] = 0
        else:
            last_used_server[slId] += 1
            last_used_server[slId] %= 3
        
        # If the server is dead, try the next one
        for _ in range(0,2): 
            if workers[the_three_servers[last_used_server[slId]].encode()] == 0:
                last_used_server[slId] += 1
                last_used_server[slId] %= 3
            else:
                break
            
        server = the_three_servers[last_used_server[slId]]
        server_to_client[server] = client_id
        handle_save_delete_request(server, request_id, client_id, request)
    
def handle_server_response(response):
    if (len(response) < 7):
        worker, empty, request_message = response[:3]
        if request_message == b"NEW_CONNECTION":
            handle_server_recovery(worker)
        elif request_message == b"HEARTBEAT" or request_message == b"NEW_SERVER":
            workers[worker] = HEARTBEAT_LIVENESS
        elif request_message == b"RECONECT":
            handle_server_recovery(worker)
        else:
            print("-> [SERVER] Received invalid response from server: ", response)
        return
    worker, empty, request_id, empty, client, empty, response_json = response[:7]
    workers[worker] = HEARTBEAT_LIVENESS

    response_dict = json.loads(response_json)
    
    # print("Received response from server: ", response)
    print(f"-> [SERVER] Received {response_dict['status']} response to request {request_id.decode('utf-8')}")
    print(f"    | Sending to client: {request_to_client[request_id.decode('utf-8')]}")

    frontend.send_multipart([request_to_client[request_id.decode('utf-8')], b"", response_json])

def main():
    initial_server_connections()
    global heartbeat_at
    while True:
        socks = dict(poller.poll(HEARTBEAT_INTERVAL * 1000))
        try:
            if socks.get(frontend) == zmq.POLLIN:
                request = frontend.recv_multipart()
                handle_client_request(request)
        except zmq.ZMQError as e:
            print(f"ZMQ Error: {e}")
        except json.JSONDecodeError as e:
            print(f"JSON Decode Error: {e}")
        except Exception as e:
            print(f"Unexpected Error: {e}")
        try:
            if socks.get(backend) == zmq.POLLIN:
                response = backend.recv_multipart()
                handle_server_response(response)
        except zmq.ZMQError as e:
            print(f"ZMQ Error: {e}")
        except json.JSONDecodeError as e:
            print(f"JSON Decode Error: {e}")
        except Exception as e:
            print(f"Unexpected Error: {e}")
        # Send heartbeats to workers
        if time.time() >= heartbeat_at:
            heartbeat_check()
           
if __name__ == "__main__":
    main()



    
