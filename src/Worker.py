import zmq
import json
import sys
from PNCRDT import PNCRDT
import time
from uhashring import HashRing
import os

if len(sys.argv) != 2:
    print("Usage: python Worker.py <server_name>")
    sys.exit(1)

context = zmq.Context()
broker = context.socket(zmq.REQ)
broker.identity = u"{}".format(sys.argv[1]).encode('utf-8')
broker.connect("tcp://127.0.0.1:5560")


def getFinalPortNumber():
    return (format(sys.argv[1])[6])

workersin = context.socket(zmq.REP)
workersin.identity = u"{}".format(sys.argv[1]).encode('utf-8')
workersin.bind("tcp://127.0.0.1:556"+getFinalPortNumber())

poller = zmq.Poller()
poller.register(broker, zmq.POLLIN)
poller.register(workersin, zmq.POLLIN)
# List of backend servers
servers = ["server1", "server2", "server3", "server4"]

ring = HashRing(servers)
shoppingLists = {}
broker_disconect = time.time() + 6

def getShoppingListItems(slId: str) -> PNCRDT:
    """
        @Returns PNCRDT data for a given Shopping List ID or None if not found
    """ 
    global shoppingLists

    print("Getting Shopping List -> ", slId)
    crdt = shoppingLists.get(slId)
    if crdt:
        return crdt.getData()
    return None
    
def saveShoppingList(slId: str, newCrdt: PNCRDT):
    """
        Saves in Key-Value Store the given PNCRDT for a given Shopping List ID
    """
    global shoppingLists

    print("Saving Shopping List -> ", slId)
    curCrdt = shoppingLists.get(slId)
    if curCrdt:
        curCrdt.merge(newCrdt)
        shoppingLists[slId] = curCrdt
        saveToFile(slId, curCrdt)
    else:
        shoppingLists[slId] = newCrdt
        saveToFile(slId, newCrdt)

def handleClientGetRequest(message, request_id, client):
    # Extract data from the message
    slId = message["slId"]

    # Get the Shopping List from the Key-Value Store
    items = getShoppingListItems(slId)

    # Send the Shopping List to the client
    if items:
        broker.send_multipart([request_id.encode(), b"", client, b"", json.dumps({"status": "found", "data": items}).encode('utf-8')])
    else:
        broker.send_multipart([request_id.encode(), b"", client, b"", json.dumps({"status": "not found", "data": {}}).encode('utf-8')])

def handleClientSaveRequest(message, request_id, client):
    # Extract data from the message
    slId = message["slId"]
    node_id = message["node_id"]
    p_counter = message["p_counter"]
    n_counter = message["n_counter"]
    data = {"node_id": node_id, "p_counter": p_counter, "n_counter": n_counter}

    # Create a new PNCRDT and load the data
    newCrdt = PNCRDT()
    newCrdt.loadData(data, slId)

    # Save the Shopping List on the Key-Value Store
    saveShoppingList(slId, newCrdt)

    # Send a response to the client
    broker.send_multipart([request_id.encode(), b"", client, b"", json.dumps({"status": "saved"}).encode('utf-8')])
    print("    | Saved Shopping List")
    
    # Merge the Shopping List with the other servers
    serverMerge(slId)

def handleClientDeleteRequest(message, request_id, client):
    # Extract data from the message
    slId = message["slId"]
    print("Deleting Shopping List -> ", slId)
    deleteFromFile(slId)
    broker.send_multipart([request_id.encode(), b"", client, b"", json.dumps({"status": "deleted"}).encode('utf-8')])
    serverDelete(slId)

def handleClientNewServerRequest(message):
    """
        Whem a new server is added to the ring, each server gets the info about the new server to add onto their ring copy.
        Also the server that comes after the new server sends him any lists they had stored that should now be in the new server
    """
    global ring, servers, shoppingLists, broker
    newWorker = message["new"]
    next_server = message["next"]
    second_next_server = message["second"]
    third_next_server = message["third"]
    broker.send_multipart([b"NEW_SERVER"])
    servers.append(newWorker)
    servers.sort()
    ring = HashRing(servers)
    if sys.argv[1] == next_server:
        lists_to_send = {}
        oldShoppingLists = shoppingLists.copy()

        for slId,list in oldShoppingLists.items():
            primary_server = ring.get_node(slId)
            index = servers.index(primary_server)
            second_server = servers[(index + 1) % len(servers)]
            third_server = servers[(index + 2) % len(servers)]
            fourth_server = servers[(index + 3) % len(servers)]
            if sys.argv[1] == fourth_server:
                lists_to_send[slId] = list.getData()
                del shoppingLists[slId]
                deleteFromFile(slId)
            elif sys.argv[1] == second_server or sys.argv[1] == third_server:
                lists_to_send[slId] = list.getData()

        if len(lists_to_send) > 0:
            workerout = context.socket(zmq.REQ)
            workerout.connect("tcp://127.0.0.1:556"+ newWorker[6])
            workerout.send(json.dumps({
                "worker": sys.argv[1], 
                "type": "save",
                "lists": lists_to_send
            }).encode('utf-8'))
            print("-> [SERVER] Sent data to new server")
            workerout.recv_json()     
    elif sys.argv[1] == second_next_server or sys.argv[1] == third_next_server:
        oldShoppingLists = shoppingLists.copy()

        for slId,list in oldShoppingLists.items():
            primary_server = ring.get_node(slId)
            index = servers.index(primary_server)
            fourth_server = servers[(index + 3) % len(servers)]
            if sys.argv[1] == fourth_server:
                del shoppingLists[slId]
                deleteFromFile(slId)

def handleClientRequests(request_id, client, message):
    message = json.loads(message)
    
    mType = message["type"]
    
    if (mType == "get"):
        handleClientGetRequest(message, request_id, client)

    elif (mType == "save"):
        handleClientSaveRequest(message, request_id, client)
    
    elif (mType == "delete"):
        handleClientDeleteRequest(message, request_id, client)
        
    elif (mType == "newServer"):
        handleClientNewServerRequest(message)

def serverMerge(slId):
    primary_server = ring.get_node(slId)
    index = servers.index(primary_server)
    the_three_servers = [primary_server,servers[(index + 1) % len(servers)], servers[(index + 2) % len(servers)]]
    lists_to_merge = []
    for server in the_three_servers:
        if server == sys.argv[1]:
            continue
        print("-> [SERVER] Sending " + server + " a request")
        #open a req socket to server3
        workerout = context.socket(zmq.REQ)
        workerout.setsockopt(zmq.RCVTIMEO, 500) 
        workerout.setsockopt(zmq.LINGER, 0)
        workerout.connect("tcp://127.0.0.1:556"+ server[6])
        try:
            workerout.send(json.dumps({
                "worker": sys.argv[1], 
                "type": "get",
                "slId": slId
            }).encode('utf-8'))
            response = workerout.recv_json()
            print("-> [SERVER] Received response from worker: ", response["status"])
            if response["status"] == "found":
                lists_to_merge.append(response["data"])
        except zmq.Again as e:
            print(f"Timeout occurred while communicating with server {server}: {e}")
        finally:
            workerout.close()
    localCrdt = shoppingLists[slId]
    while len(lists_to_merge) > 0:
        list_to_merge = lists_to_merge.pop()
        newCrdt = PNCRDT()
        newCrdt.loadData(list_to_merge, slId)
        localCrdt.merge(newCrdt)
    saveShoppingList(slId, localCrdt)
    #send the merged list to the other 2 servers
    for server in the_three_servers:
        if server == sys.argv[1]:
            continue
        print("-> [SERVER] Sending " + server + " a shopping list")
        #open a req socket to server3
        workerout = context.socket(zmq.REQ)
        workerout.connect("tcp://127.0.0.1:556"+ server[6])
        try:
            workerout.send(json.dumps({
                "worker": sys.argv[1], 
                "type": "save",
                "lists": {slId: localCrdt.getData()}
            }).encode('utf-8'))
        except zmq.Again as e:
            print(f"{server} may have not received the merged list: {e}")
        finally:
            workerout.close()
def serverDelete(slId):
    primary_server = ring.get_node(slId)
    index = servers.index(primary_server)
    the_three_servers = [primary_server,servers[(index + 1) % len(servers)], servers[(index + 2) % len(servers)]]
    for server in the_three_servers:
        if server == sys.argv[1]:
            continue
        print("-> [SERVER] Sending " + server + " a request")
        #open a req socket to server3
        workerout = context.socket(zmq.REQ)
        workerout.setsockopt(zmq.RCVTIMEO, 500)
        workerout.setsockopt(zmq.LINGER, 0)
        workerout.connect("tcp://127.0.0.1:556"+ server[6])
        try:
            workerout.send(json.dumps({
                "worker": sys.argv[1], 
                "type": "delete",
                "slId": slId
            }).encode('utf-8'))
            response = workerout.recv_json()
            print("-> [SERVER] Received response from Server: ", response)
        except zmq.Again as e:
            print(f"{server} may have not deleted the list {e}")
        finally:
            workerout.close()
def handleWorkerRequests(message):
    originalWorker = message["worker"]
    mType = message["type"]
    
    
    if (mType == "get"):
        slId = message["slId"]
        items = getShoppingListItems(slId)

        print("-> [SERVER] Got Shopping List with data", items)
        if items:
            workersin.send(json.dumps({"status": "found", "data": items}).encode('utf-8'))
        else:
            # if not found, send a not found message and an empty list
            workersin.send(json.dumps({"status": "not found", "data": {}}).encode('utf-8'))

    elif (mType == "save"):
        lists = message["lists"]
        for slId,list_data in lists.items():
            newCrdt = PNCRDT()
            newCrdt.loadData(list_data, slId)	
            saveShoppingList(slId, newCrdt)
        workersin.send(json.dumps({"status": "saved"}).encode('utf-8'))
        print("-> [SERVER] Sent saving confirmation to Server")
    elif (mType == "delete"):
        slId = message["slId"]
        deleteFromFile(slId)
        workersin.send(json.dumps({"status": "deleted"}).encode('utf-8'))
        print("-> [SERVER] Sent deletion confirmation to Server")

def resetBroker(socket):
    global poller, workersin

    socket.close()
    socket = context.socket(zmq.REQ)
    socket.identity = u"{}".format(sys.argv[1]).encode('utf-8')
    socket.connect("tcp://127.0.0.1:5560")

    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    poller.register(workersin, zmq.POLLIN)
    return socket

def resetWorkers(socket):
    global poller, broker

    socket.close()
    socket = context.socket(zmq.REP)
    socket.connect("tcp://127.0.0.1:556"+getFinalPortNumber())
    socket.bind("tcp://127.0.0.1:556"+getFinalPortNumber())

    poller = zmq.Poller()
    poller.register(broker, zmq.POLLIN)
    poller.register(socket, zmq.POLLIN)
    return socket

def handle_broker_recovery():
    global broker, broker_disconect

    while True:
        try:
            print("-> [BROKER] Sending Reconect request to the broker")
            broker.send(b"RECONECT")
            client, empty, request = broker.recv_multipart()
            print("   | [BROKER] Received response: ", request)
            if request == b"READY":
                broker.send(b"READY")
                broker_disconect = time.time() + 7
                break
        except zmq.ZMQError as e:
            print(f"ZMQ Error: {e}")
            broker = resetBroker(broker)

def saveToFile(slId: str, crdt: PNCRDT):
    try:
        file_path = os.path.join(os.path.dirname(__file__), 'servers', sys.argv[1] + ".json")
        try:
            with open(file_path, "r") as file:
                data = json.load(file)
        except FileNotFoundError:
            data = {}
        
        save = {"node_id": crdt.get_node_id(), "p_counter": crdt.p_counter, "n_counter": crdt.n_counter}

        if slId in data:
            data[slId].update(save)
        else:
            data[slId] = save

        # Write the updated data back to the file
        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)
    except Exception as e:
        print(f"    | Failed to save data: {e}")

def deleteFromFile(slId: str):
    try:
        file_path = os.path.join(os.path.dirname(__file__), 'servers', sys.argv[1] + ".json")
        try:
            with open(file_path, "r") as file:
                data = json.load(file)
        except FileNotFoundError:
            data = {}
        
        # Delete the Shopping List from the data
        if slId in data:
            del data[slId]
        
        # Write the updated data back to the file
        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)
    except Exception as e:
        print(f"    | Failed to save data: {e}")

def loadFromFile():
    """
        Loads all shopping lists from the file. If the file does not exist, it creates the file.
    """
    global shoppingLists

    file_path = os.path.join(os.path.dirname(__file__), 'servers', sys.argv[1] + ".json")
    try:
        with open(file_path, "r") as file:
            data = json.load(file)
            print("    | Loaded shopping lists from file")
            for slId, list_data in data.items():
                crdt = PNCRDT(slId)
                crdt.loadData(list_data, slId)
                shoppingLists[slId] = crdt

    except FileNotFoundError:
        print("    | File not found, creating a new one")
        with open(file_path, "w") as file:
            json.dump({}, file, indent=4)
    except Exception as e:
        print(f"    | Failed to load data: {e}")


def main():
    global broker_disconect, broker, workersin, poller
    loadFromFile()

    # Tell broker we're ready for work
    broker.send(b"NEW_CONNECTION")
    while True:
        client, empty, request = broker.recv_multipart()
        print("-> [BROKER] Received request from broker: ", request)
        if request == b"READY":
            broker.send(b"READY")
            break

    while True:
        # Debug disconnect to save time on testing
        if time.time() >= broker_disconect:
            print("-> [BROKER] Broker disconected") 
            handle_broker_recovery()
            print("-> [BROKER] Broker reconected")
        socks = dict(poller.poll(timeout=5000))           
        try:
            if socks.get(broker) == zmq.POLLIN:
                request_id, empty, client, empty, request = broker.recv_multipart()                
                # Heartbeat
                broker_disconect = time.time() + 6
                if request == b"HEARTBEAT":
                    broker.send_multipart([b"HEARTBEAT"])
                else:
                    # Handle request
                    print("-> [BROKER] Received request from client: ", client)
                    handleClientRequests(request_id.decode('utf-8'), client, request.decode('utf-8'))
        except zmq.ZMQError as e:
            print(f"ZMQ Error: {e}")
            broker = resetBroker(broker)
        except json.JSONDecodeError as e:
            print(f"JSON Decode Error: {e}")
            broker = resetBroker(broker)
        except Exception as e:
            print(f"Unexpected Error: {e}")
            broker = resetBroker(broker) 
        try:
            if socks.get(workersin) == zmq.POLLIN:
                request = workersin.recv_json()
                print("-> [SERVER] Received request from Server: ", request["worker"])
                handleWorkerRequests(request)
        except zmq.ZMQError as e:
            print(f"ZMQ Error: {e}")
            workersin = resetWorkers(workersin)
        except json.JSONDecodeError as e:
            print(f"JSON Decode Error: {e}")
            workersin = resetWorkers(workersin)
        except Exception as e:
            print(f"Unexpected Error: {e}")
            workersin = resetWorkers(workersin)
if __name__ == "__main__":
    main()
    

    
'''
cd Documents/Github/g15/src && python Worker.py server
'''



