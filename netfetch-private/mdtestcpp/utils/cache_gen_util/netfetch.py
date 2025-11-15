from collections import defaultdict
from anytree import Node, RenderTree, NodeMixin
from common import parse_filename

# some constants
HOT_REPORT_THRESHOLD = 1
SCALE_COEFFICIENT = 0.5

is_fs_tree_build = False
root_path = '/#test-dir.0/mdtest_tree.0'
root = Node(root_path)

cached = defaultdict(int)
uncached = defaultdict(int)


def compute_capacity_for_hot_report(filename):
    global cached
    cached_capacity = 0
    entries = parse_filename(filename)
    worst_capacity = len(entries)
    for e in entries:
        if e in cached:
            cached_capacity +=1
        else:
            break
    return worst_capacity - cached_capacity

def build_fs_tree_for_warmup(filename):
    global root
    parent_node = root
    entries = parse_filename(filename)
    # build the tree
    for e in entries:
        if e == root_path:
            continue
        current = Node(e, parent=parent_node)
        parent_node = current

def update_fs_tree_for_warmup(filename):
    global root
    parent_node = root
    entries = parse_filename(filename)
    # update the tree
    for e in entries:
        # if already has the node, skip
        if e == root_path:
            continue
        # e: str; children: node
        is_internal_path_cached = False
        for child in parent_node.children:
            if e == child.name:
                parent_node = child
                is_internal_path_cached = True
                break
        if is_internal_path_cached==False:
            current = Node(e, parent=parent_node)
            parent_node = current


def compute_scaled_freq(leaf):
    free_space = 1
    while leaf.parent != None:
        if len(leaf.parent.children)==1:
            free_space += 1
        else:
            break
        leaf = leaf.parent
    # obtain weight from cached dict
    weight = cached[leaf.name]
    return [weight/(free_space*SCALE_COEFFICIENT), free_space]


# warmup
def handle_entry(filename, cache_capacity):
    global is_fs_tree_build, cached, uncached
    is_stop = False
    capacity = compute_capacity_for_hot_report(filename)
    if uncached[filename] >= 0:
    # if uncached[filename] >= HOT_REPORT_THRESHOLD*capacity*SCALE_COEFFICIENT:
        if len(cached)+capacity >= cache_capacity:
            is_stop = True
            return is_stop
        # print("hot path: ", filename)
        uncached[filename] = -1
        # admission
        hot_entries = parse_filename(filename)
        hot_entries.reverse()
        for i in range(capacity):
            cached[hot_entries[i]] = 0
        if is_fs_tree_build == False:
            build_fs_tree_for_warmup(filename)
            is_fs_tree_build = True
        else:
            update_fs_tree_for_warmup(filename)
    elif uncached[filename] == -1:
        cached[filename] += 1
    else:
        uncached[filename] += 1
    return is_stop

def netfetch_handle_cache(cache_capacity, lines_for_warmup, filenames):
    for idx in lines_for_warmup:
        filename = filenames[int(idx)].strip()
        is_stop = handle_entry(filename, cache_capacity)
        if is_stop:
            break
    # print("After admission, len(cached):", len(cached))
    return cached
