import ast
from typing import List, Dict
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import pandas as pd


class Action:
    def __init__(self, time: int, reward: str, host: str, keys: List[str]):
        self.time = time
        self.reward = reward
        self.host = host
        self.keys = keys

    def __str__(self):
        return f"Action(time={self.time}, reward={self.reward}, host={self.host}, keys={self.keys})"

    def __repr__(self):
        return str(self)


class NodeValue:
    def __init__(self, required_time: int, required_keys: List[str], required_hosts: List[str], host: str):
        self.required_time = required_time
        self.required_keys = required_keys
        self.required_hosts = required_hosts
        self.host = host

    def __str__(self):
        return f"NodeValue(required_time={self.required_time}, required_keys={self.required_keys}, required_hosts={self.required_hosts}, host={self.host})"

    def __repr__(self):
        return str(self)


class TreeNode:
    def __init__(self, value: NodeValue, parent=None):
        self.parent = parent
        self.value = value
        self.children = []
        self.level = parent.level + 1 if parent else -1

    def __str__(self):
        return f"TreeNode(value={self.value}, level={self.level})"

    def __repr__(self):
        return str(self)

    def copy(self, new_parent, depth):
        result = TreeNode(
            NodeValue(
                self.value.required_time,
                self.value.required_keys,
                self.value.required_hosts,
                self.value.host
            ),
            new_parent
        )
        result.level = new_parent.level + 1

        if depth == 0:
            return result

        child_copy = [child.copy(result, depth - 1) for child in self.children]
        result.children = child_copy
        return result


class RiskTree:
    def __str__(self):
        return f"RiskTree(root={self.root})"

    def __repr__(self):
        return str(self)

    max_depth = 20

    def __init__(self, root_value: NodeValue = None, root: TreeNode = None):
        self.root = root if root else TreeNode(root_value)
        self.search_in_tree_dict = defaultdict(list)
        self.search_in_forest_tree = []

    def add_child(self, parent: TreeNode, child_value: NodeValue):
        if not parent:
            raise ValueError("Parent cannot be None")

        node = TreeNode(child_value, parent)
        self.search_in_tree_dict[child_value.host].append(node)
        parent.children.append(node)

    def traverse(self, node: TreeNode, action, current_depth=0):
        if not node or current_depth > self.max_depth:
            return

        action(node)

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.traverse, child, action, current_depth + 1) for child in node.children]
            for future in futures:
                future.result()

    def copy(self, reversed_depth, future_parent):
        self.search_in_forest_tree.append(future_parent)
        return self.root.copy(future_parent, reversed_depth)

    def search(self, risk_object):
        return self.search_in_tree_dict[risk_object]


def create_action_forest(actions: List[Action]) -> Dict[str, RiskTree]:
    forest = defaultdict(RiskTree)
    for action in actions:
        root = TreeNode(None)
        tree = RiskTree(root)
        action_node = NodeValue(action.time, action.keys, [action.host], action.host)
        tree.add_child(root, action_node)
        forest[action.reward] = tree
    return forest


def extend_action_forest(forest: Dict[str, RiskTree]) -> Dict[str, RiskTree]:
    for risk_tree in forest.values():
        for tree in risk_tree.search_in_forest_tree:
            tree_copy = forest[tree.value.required_hosts[0]].copy(RiskTree.max_depth - tree.level, tree)
            tree_copy.search_in_forest_tree = tree.search_in_forest_tree
    return forest


def transform_action_forest_to_path_forest(forest: Dict[str, RiskTree]) -> Dict[str, RiskTree]:
    for risk_tree in forest.values():
        risk_tree.traverse(risk_tree.root, aggregate_from_parent)
    return forest


def aggregate_from_parent(node: TreeNode):
    if not node.parent or not node.value:
        return

    value = node.value
    parent_value = node.parent.value

    value.required_keys.extend(parent_value.required_keys)
    value.required_hosts.extend(parent_value.required_hosts)
    value.required_time += parent_value.required_time


if __name__ == "__main__":
    df = pd.read_csv("generated_network.csv")


    def convert_to_list(column):
        def safe_literal_eval(value):
            if isinstance(value, str):
                try:
                    return ast.literal_eval(value)
                except (ValueError, SyntaxError) as e:
                    print(f"Error parsing value: {value} - {e}")
                    return value
            return value

        return column.apply(safe_literal_eval)


    # Convert the string representations of lists to actual lists
    df['out_host'] = convert_to_list(df['out_host'])
    df['key'] = convert_to_list(df['key'])
    df['time'] = convert_to_list(df['time'])

    # Group by target_host and aggregate the columns into lists
    grouped = df.groupby(by='target_host').agg(lambda x: x.tolist()).reset_index()

    # Convert the grouped DataFrame to a dictionary
    result_dict = {}

    for _, row in grouped.iterrows():
        target_host = row['target_host']
        out_hosts = row['out_host']
        keys = row['key']
        times = row['time']

        result_dict[target_host] = [
            {'out_host': out_host, 'key': key, 'time': time}
            for out_host, key, time in zip(out_hosts, keys, times)
        ]

    actions = []

    # Action(10, "reward1", "host1", ["key1"]),
    # Action(20, "reward2", "host2", ["key2"]),
    # Action(30, "reward3", "host3", ["key3"]),
    # ...

    for key, value in result_dict.items():
        for val in value:
            print(key, value, val)
            action = Action(int(val['time']), key, val['out_host'], [val['key']])
            actions.append(action)

    forest = create_action_forest(actions)
    forest = extend_action_forest(forest)
    forest = transform_action_forest_to_path_forest(forest)

    for key, value in forest.items():
        print(key, value)
