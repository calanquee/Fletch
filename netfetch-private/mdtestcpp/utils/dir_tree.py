from collections import defaultdict, deque
import itertools

def gen_dir_tree(depth, branch, base_dir):
    def generate_tree_with_paths(depth, branch):
        if depth <= 0:
            raise ValueError("Depth must be greater than 0")
        if branch <= 0:
            raise ValueError("Branch factor must be greater than 0")

        node_id = 0
        queue = deque([(node_id, [])])  # (node_id, path_to_node)
        tree = defaultdict(list)
        dir_tree = []

        while queue:
            current_node, path_to_node = queue.popleft()
            current_path = path_to_node + [current_node]
            path_str = f"{base_dir}{''.join([f'/mdtest_tree.{num}' for num in current_path])}"
            dir_tree.append(path_str)

            if len(path_to_node) < depth - 1:  # Generate child nodes if depth not yet reached
                for i in range(branch):
                    node_id += 1
                    tree[current_node].append(node_id)
                    queue.append((node_id, current_path))
        
        return dir_tree

    # Adjust depth to match the requested structure
    adjust_depth = depth - 1
    return generate_tree_with_paths(adjust_depth, branch)
     
# # 
# fs_tre = gen_dir_tree(
#             10,
#             4,
#             f"/#test-dir.0.d.{8}.n.{63962892}.b.{4}",
#         )
# for item in fs_tre:
#     print(item)