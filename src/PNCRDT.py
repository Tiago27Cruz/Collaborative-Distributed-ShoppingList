# This file contains an implementation of a Positive-Negative Counter CRDT.

from typing import Dict
import json

class PNCRDT:
    def __init__(self, node_id: str = "defaultNode", sl_id: str = "defaultSL"):
        self.node_id = node_id
        self.sl_id = sl_id
        # Separate increment and decrement counters for each item
        self.p_counter: Dict[str, Dict[str, int]] = {}
        self.n_counter: Dict[str, Dict[str, int]] = {}
    
    """
    Add quantity to the item.
    """
    def add(self, key: str, quantity: int):
        if key not in self.p_counter:
            self.p_counter[key] = {}
        if(self.node_id not in self.p_counter[key]):
            self.p_counter[key][self.node_id] = quantity
        else: self.p_counter[key][self.node_id] += quantity
    
    """
    Remove quantity from the item.
    """
    def remove(self, key: str, quantity: int):
        if key not in self.n_counter:
            self.n_counter[key] = {}
        if(self.node_id not in self.n_counter[key]):
            self.n_counter[key][self.node_id] = quantity
        else: self.n_counter[key][self.node_id] += quantity
    
    def delete(self, key: str):

        if key not in self.p_counter:
            print(f"    | No {key} found in the list in order to delete")
            return

        # Calculate the effective quantity of the item
        p_counter = self.p_counter.get(key, {})
        n_counter = self.n_counter.get(key, {})

        total_quantity = sum(p_counter.values()) - sum(n_counter.values())

        if key not in self.n_counter:
            self.n_counter[key] = {}

        if total_quantity > 0:
        # Add the effective quantity to the negative counter
            if self.node_id not in n_counter:
                self.n_counter[key][self.node_id] = total_quantity
            else:
                self.n_counter[key][self.node_id] += total_quantity

    """
    Merge the state of this PN-Counter with another PN-Counter.
    """
    def merge(self, other: 'PNCRDT'):
        # Merge P-counters
        for key, node_counters in other.p_counter.items():
            for node, count in node_counters.items():
                p_counter = self.p_counter.get(key, {})
                if node not in p_counter or p_counter.get(node, 0) < count:
                    if key not in self.p_counter:
                        self.p_counter[key] = {}
                    self.p_counter[key][node] = count
        
        # Merge N-counters
        for key, node_counters in other.n_counter.items():
            for node, count in node_counters.items():
                n_counter = self.n_counter.get(key, {})
                if node not in n_counter or n_counter.get(node, 0) < count:
                    if key not in self.n_counter:
                        self.n_counter[key] = {}
                    self.n_counter[key][node] = count

    """
    Get the current state of the items.
    """
    def get_state(self) -> Dict[str, int]:
        state = {}
        for key in set(self.p_counter.keys()).union(self.n_counter.keys()):
            if key in self.p_counter:
                if key not in self.n_counter:
                    total_quantity = sum(self.p_counter[key].values())
                else:
                    total_quantity = sum(self.p_counter[key].values()) - sum(self.n_counter[key].values())
                if total_quantity > 0:
                    state[key] = total_quantity
        return state
    
    """
    Get the P-counters and N-counters.
    """
    def get_counters(self) -> Dict[str, Dict[str, int]]:
        return {
            "p_counter": self.p_counter,
            "n_counter": self.n_counter
        }

    def get_node_id(self) -> str:
        return self.node_id
    
    def get_sl_id(self) -> str:
        return self.sl_id

    def convertToJson(self):
        """
            Converts the CRDT state to JSON format
        """
        node_id = self.node_id
        sl_id = self.sl_id
        counters = self.get_counters()
        return json.dumps({sl_id: {"node_id": node_id, "p_counter": counters["p_counter"], "n_counter": counters["n_counter"]}})
    
    def loadData(self, data: dict, sl_id: str):
        """
            Loads the CRDT state from JSON format
        """
        self.node_id = data["node_id"]
        self.sl_id = sl_id
        self.p_counter = data["p_counter"]
        self.n_counter = data["n_counter"]
    
    def getData(self):
        """
            Returns the CRDT state as a dictionary
        """
        return {"node_id": self.node_id, "p_counter": self.p_counter, "n_counter": self.n_counter}

    def __str__(self):
        state = self.get_state()
        return f"Shopping List: {self.sl_id}\n{json.dumps(state, indent=4)}"

    def __repr__(self):
        return f"PNCRDT(node_id={self.node_id}, sl_id={self.sl_id}, p_counter={self.p_counter}, n_counter={self.n_counter})"
