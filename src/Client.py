import zmq
from PNCRDT import PNCRDT
import json
import hashlib
import random
import sys
import os

RETRIES = 3
REQUEST_TIMEOUT = 5000

# ZMQ Related Functions

def connect() -> zmq.SyncSocket:
    """
        Connects to the broker and returns the socket and poller
    """
    #  Prepare our context and sockets
    context = zmq.Context()

    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5559")
    socket.setsockopt(zmq.RCVTIMEO, REQUEST_TIMEOUT)
    socket.setsockopt(zmq.SNDTIMEO, REQUEST_TIMEOUT)
    socket.setsockopt(zmq.REQ_RELAXED, 1)
    #socket.setsockopt(zmq.REQ_CORRELATE, 1)

    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)

    return socket, poller

    

def sendRequest(socket: zmq.SyncSocket, mtype: str, slId:str, node_id:str, p_counter: dict, n_counter: dict):
    """
        Sends a request to the broker with the given data formatted as needed by the Workers
    """
    print(f"-> Sending {mtype} request")
    try:
        socket.send_json({
            "type": mtype,
            "slId": slId,
            "node_id": node_id,
            "p_counter": p_counter,
            "n_counter": n_counter
        })
        return True
    except zmq.Again as e:
        print(f"    | Request timed out: {e}")
        socket.setsockopt(zmq.LINGER, 0)
        return False
    except Exception as e:
        print(f"    | Request failed: {e}")
        socket.setsockopt(zmq.LINGER, 0)
        return False
    
def getRequest(socket: zmq.SyncSocket, poller, slId: str, node_id:str, retry: int) -> PNCRDT:
    """
        Sends a GET request to the broker to get the shopping list with the given id.
        Waits for broker response with the Shopping List data
        Response format: {"status": "found", "data": {"node_id": _, "p_counter": _, "n_counter": _}} or {"status": "not found", "data": {}}
        If the server could not be reached, the response will be "A server could not be reached"
    """
    if retry == 0:
        print("    | Maximum retries reached")
        return None

    req = sendRequest(socket, "get", slId, node_id, {}, {})
    if req is False:
        return None

    socks = dict(poller.poll(timeout=REQUEST_TIMEOUT))
    response = None

    while response is None:
        if socks.get(socket) == zmq.POLLIN:
            response = socket.recv_multipart()[0].decode("utf-8")

            if response == "A server could not be reached":
                print("    | A server could not be reached")
                return None

            response = json.loads(response)

            print(f"    | Received response: {response}")
            if response["status"] == "found":
                crdt = PNCRDT()
                crdt.loadData(response["data"], slId)
                return crdt
            else:
                print("    | Shopping list not found")
                return None
        else:
            print("    | Timeout waiting for GET response")
            socket.setsockopt(zmq.LINGER, 0)
            return getRequest(socket, poller, slId, node_id, retry - 1)
    
def saveRequest(socket: zmq.SyncSocket, poller, slId, node_id, p_counter, n_counter, retry):
    """
        Sends the shopping list to the server to save it.
    """
    if retry == 0:
        print("    | Maximum retries reached")
        return False

    req = sendRequest(socket, "save", slId, node_id, p_counter, n_counter)
    if req is False:
        return None

    socks = dict(poller.poll(timeout=REQUEST_TIMEOUT))
    response = None

    while response is None:
        if socks.get(socket) == zmq.POLLIN:
            response = socket.recv_json()
            print(f"    | Received response: {response}")
            return True
        else:
            print("    | Timeout waiting for SAVE response")
            socket.setsockopt(zmq.LINGER, 0)
            return saveRequest(socket, poller, slId, node_id, p_counter, n_counter, retry - 1)

def deleteRequest(socket: zmq.SyncSocket, poller, slId, retry):
    """
        Sends a delete request to the server to delete the shopping list with the given id.
    """
    if retry == 0:
        print("    | Maximum retries reached")
        return False

    req = sendRequest(socket, "delete", slId, "", {}, {})
    if req is False:
        return False

    socks = dict(poller.poll(timeout=REQUEST_TIMEOUT))
    response = None

    while response is None:
        if socks.get(socket) == zmq.POLLIN:
            response = socket.recv_json()
            print(f"    | Received response: {response}")
            return True
        else:
            print("    | Timeout waiting for DELETE response")
            socket.setsockopt(zmq.LINGER, 0)
            return deleteRequest(socket, poller, slId, retry - 1)



# Local Storage Functions

def saveToFile(slId: str, node_id: str, counters: dict):
    """
        Saves the shopping list data to a file with the name of the node_id given.
    """
    try:
        try:
            file_path = os.path.join(os.path.dirname(__file__), 'clients', node_id + ".json")
            with open(file_path, "r") as file:
                data = json.load(file)
        except FileNotFoundError:
            data = {}
            with open(file_path, "w") as file:
                json.dump({}, file, indent=4)
        
        save = {"node_id": node_id, "p_counter": counters["p_counter"], "n_counter": counters["n_counter"]}
        print(f"-> Saving locally")

        if slId in data:
            data[slId].update(save)
        else:
            data[slId] = save

        # Write the updated data back to the file
        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)
    except Exception as e:
        print(f"    | Failed to save data: {e}")

def loadFromFile(slId: str, node_id: str) -> PNCRDT:
    """
        Tries to load a shopping list from a file with the name of the id given.\n
        Returns the json data or None if the file was not found.
    """
    try:
        file_path = os.path.join(os.path.dirname(__file__), 'clients', node_id + ".json")
        with open(file_path, "r") as file:
            file = file.read()
            data = json.loads(file)
            crdt = PNCRDT()
            crdt.loadData(data[slId], slId)
            return crdt
    except FileNotFoundError:
        print("    | File not found, creating a new one")
        with open(file_path, "w") as file:
            json.dump({}, file, indent=4)
    except Exception as e:
        return None

# Shopping List Functions

def generateId():
    """
        Generates a random hashed 16 bit string id for the shopping list
    """
    return hashlib.md5(str(random.randint(0, 1000000)).encode()).hexdigest()
    
def sync(crdt: PNCRDT, stored: PNCRDT, response: PNCRDT) -> PNCRDT:
    """
        Syncs the stored PNCRDT with the responses received from the server
    """
    print("-> Syncing data")
    if(response is None):
        if stored is None:
            print("    | No response received & No local storage found. Please insert a valid ID")
            return None
        else:
            print("    | No response received -> Loading only local storage")
            node_id = crdt.get_node_id()
            crdt = stored
            stored.node_id = node_id
            return stored   

    if stored is not None:
        node_id = crdt.get_node_id()
        crdt = stored
        stored.node_id = node_id

    print("    | Merging local storage with response")
    crdt.merge(response)
    
    return crdt

def loadShoppingList(socket, poller, crdt: PNCRDT):
    """
        Loads a shopping list from the server and the local storage and merges both using CRDT
    """
    sl_id = crdt.get_sl_id()
    node_id = crdt.get_node_id()
    response = getRequest(socket, poller, sl_id, node_id, RETRIES)
    stored = loadFromFile(sl_id, node_id)

    return sync(crdt, stored, response)

def createShoppingList(nodeId):
    """
        Given a node id, creates a new shopping list with a random id
    """
    slId = generateId()
    crdt = PNCRDT(node_id=nodeId, sl_id=slId)
    return crdt

def deleteShoppingList(socket, poller, crdt):
    """
        Deletes the shopping list from the server and the local storage
    """
    # Send delete request
    slId = crdt.get_sl_id()
    node_id = crdt.get_node_id()
    deleteRequest(socket, poller, slId, RETRIES)

    try:
        file_path = os.path.join(os.path.dirname(__file__), 'clients', node_id + ".json")
        with open(file_path, "r") as file:
            data = json.load(file)
        data.pop(slId)
        
        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)
        
        print("    | Deleted shopping list")
    except Exception as e:
        print(f"    | Failed to delete data: {e}")

def slInteraction(crdt: PNCRDT, operations: list[str]) -> PNCRDT:
    """
        Given a list of operations in the format [add|remove item quantity], applies them to the CRDT
    """
    print("-> Applying operations")

    i = 0
    while i < len(operations):
        operation1 = operations[i]
        operation2 = operations[i + 1] if i + 1 < len(operations) else None
        operation3 = operations[i + 2] if i + 2 < len(operations) else None

        if operation1 == "add":
            
            if operation2:
                if operation3 and operation3.isdigit():
                    print(f"    | Adding {operation3} of {operation2}")
                    crdt.add(operation2, int(operation3))
                elif operation3 and not operation3.isdigit():
                    print(f"    | Invalid quantity for {operation1} {operation2} {operation3}")
            i += 3

        elif operation1 == "remove":
            if operation2:
                if operation3 and operation3.isdigit():
                    print(f"    | Removing {operation3} of {operation2}")
                    crdt.remove(operation2, int(operation3))
                elif operation3 and not operation3.isdigit():
                    print(f"    | Invalid quantity for {operation1} {operation2} {operation3}")
            i += 3

        elif operation1 == "delete":
            if operation2:
                print(f"    | Deleting {operation2}")
                crdt.delete(operation2)
            i += 2

        else:
            print(f"    | Invalid operation {operation1} {operation2} {operation3}")
            print("    | Operations must be in the format: [add|remove item quantity] [add|remove item quantity] ...")
            i += 1

    return crdt

def main(*args):
    socket, poller = connect()
    usage = args[0]

    if(usage == "create"):
        nodeID = args[1]
        operations = args[2:]
        
        synced_crdt = createShoppingList(nodeID)
        slId = synced_crdt.get_sl_id()
        print(f"-> Created shopping list with ID: {slId}")
    elif(usage == "use"):
        if len(args) < 4:
            print("Usage: python Client.py use listid userid [operations]")
            return
        slId = args[1]
        nodeID = args[2]
        operations = args[3:]

        crdt = PNCRDT(node_id=nodeID, sl_id=slId)
        synced_crdt = loadShoppingList(socket, poller, crdt)
        if (synced_crdt is None):
            print("    | No shopping list found. Ending")
            return
    elif(usage == "view"):
        slId = args[1]
        nodeID = args[2]
        crdt = PNCRDT(node_id=nodeID, sl_id=slId)
        synced_crdt = loadShoppingList(socket, poller, crdt)
        if (synced_crdt is None):
            print("    | No shopping list found. Ending")
            return
        print(synced_crdt)
        counters = synced_crdt.get_counters()
        saveToFile(slId, nodeID, counters)
        return
    elif (usage == "delete"):
        slId = args[1]
        nodeID = args[2]
        crdt = PNCRDT(node_id=nodeID, sl_id=slId)
        synced_crdt = loadShoppingList(socket, poller, crdt)
        if (synced_crdt is None):
            print("    | No shopping list found. Ending")
            return
        deleteShoppingList(socket, poller, synced_crdt)
        return
        
    else:
        print("Usage: python Client.py <create | use listid | view listid | delete listid> userid [operations]")
        return

    synced_crdt = slInteraction(synced_crdt, operations)

    counters = synced_crdt.get_counters()

    saveRequest(socket, poller, slId, nodeID, counters["p_counter"], counters["n_counter"], RETRIES)
    saveToFile(slId, nodeID, counters)

    print("\n    Client finished\n\n")

    socket.disconnect("tcp://localhost:5559")
    socket.close()

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python Client.py <create | use listid | view listid | delete listid> userid [operations]")
        sys.exit(1)
    main(*sys.argv[1:])
