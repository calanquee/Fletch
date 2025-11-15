#ifndef FSTREE_H
#define FSTREE_H

#include <algorithm>
#include <list>
#include <mutex>
#include <queue>
#include <sstream>
#include <stack>
#include <unordered_map>
#include <unordered_set>

#include "common_impl.h"
#include "switchos.h"

// Qingxiu: a file system structure for solving knapsack problem
class TreeNode {
public:
    std::string name;
    int weight;
    TreeNode* parent;
    std::unordered_map<std::string, TreeNode*> children;
    double scaled_frequency;
    std::list<std::tuple<TreeNode*, double, int>>::iterator ptr_in_sorted_list;

    TreeNode(const std::string& name, int weight, TreeNode* parent = nullptr, double scaled_frequency = 0.0, const std::list<std::tuple<TreeNode*, double, int>>::iterator end_iter = std::list<std::tuple<TreeNode*, double, int>>::iterator())
        : name(name), weight(weight), parent(parent), scaled_frequency(scaled_frequency), ptr_in_sorted_list(end_iter) {
#ifdef NETFETCH_POPSERVER_DEBUG
        std::cout << "[DEBUG] TreeNode created: " << name << " with weight " << weight << std::endl;
#endif
    }

    void add_child(TreeNode* child) {
        auto it = children.find(child->name);
        // children.push_back(child);
        if (it == children.end()) {
            children[child->name] = child;
            child->parent = this;
        } else {
            delete child;
        }
    }

    ~TreeNode() {
        for(auto& child : children) {
            delete child.second;
        }
        children.clear();
    }
};

TreeNode* root;
std::list<std::tuple<TreeNode*, double, int>> sorted_path_scaled_frequency_space_tuples;  // Qingxiu: maintain a sorted list for the periodically loading freq.

// Qingxiu: modify this to NOT use nodes because it incurs high computation overhead
TreeNode* build_tree(const std::vector<std::pair<std::string, int>>& path_access_pairs, int root_weight) {
    TreeNode* root = new TreeNode(root_path_for_fs_tree, root_weight, nullptr, 0.0, sorted_path_scaled_frequency_space_tuples.end());  // fix bug in iterator
    for (std::vector<std::pair<std::string, int>>::const_iterator it = path_access_pairs.begin();
         it != path_access_pairs.end(); ++it) {
        const std::pair<std::string, int>& pair = *it;
        std::string path = pair.first;
        int weight = pair.second;
        std::istringstream iss(path);
        std::string part;
        TreeNode* current_node = root;
        std::string current_path;
        // printf("processing path: %s\n", path.c_str());
        while (std::getline(iss, part, '/')) {
            if (!part.empty()) {
                current_path += "/" + part;
                // printf("current_path: %s\n", current_path.c_str());
                // printf("current_node->name: %s\n", current_node->name.c_str());
                // Qingxiu: check if the child exists
                bool found = false;
                auto it = current_node->children.find(current_path);
                if (it != current_node->children.end()) {
                    current_node = it->second;
                    found = true;
                }
                // for (auto& child : current_node->children) {
                //     if (child->name == current_path) {
                //         current_node = child;
                //         found = true;
                //         break;
                //     }
                // }
                if (!found) {
                    TreeNode* new_node = new TreeNode(current_path, 0, nullptr, 0.0, sorted_path_scaled_frequency_space_tuples.end());
                    current_node->add_child(new_node);
                    current_node = new_node;
                }
            }
        }
        current_node->weight = weight;
    }
    return root;
}

void print_tree(const TreeNode* node, const std::string& prefix = "") {
    if (!node)
        return;
    std::cout << prefix << node->name << " (weight: " << node->weight << ", scaled_freq: " << node->scaled_frequency << ")" << std::endl;
    // for (const auto& child : node->children) {
    //     print_tree(child, prefix + "    ");
    // }
    for(auto& child : node->children) {
        print_tree(child.second, prefix + "    ");
    }
}



void new_update_scaled_frequency_for_leaf_nodes(
    TreeNode* node,
    std::list<std::tuple<TreeNode*, double, int>>& sorted_path_scaled_frequency_space_tuples,
    uint32_t* frequency_counters) {

    if (!node) return;

    if (node->children.empty()) {
        int free_space = 1;
        TreeNode* current = node;
        while (current->parent != nullptr) {
            if (current->parent->children.size() == 1) {
                free_space += 1;
                current = current->parent;
            } else {
                break;
            }
        }
        // obtain weight of current node
        int tmp_idx_for_frequency_counter = 0;
        netreach_key_t key = generate_key_t_for_cachepop(node->name);
        auto it = switchos_popworker_cached_keyset.find(KeyWithPath(key, node->name.length(), node->name.c_str()));
        if (it != switchos_popworker_cached_keyset.end()) {
            tmp_idx_for_frequency_counter = it->idx_for_latest_deleted_vallen;
        }
        int tmp_frequency = frequency_counters[tmp_idx_for_frequency_counter];
        node->scaled_frequency = tmp_frequency / static_cast<double>(free_space);

        auto ptr_in_list = sorted_path_scaled_frequency_space_tuples.emplace(
            sorted_path_scaled_frequency_space_tuples.end(), node, node->scaled_frequency, free_space);
        node->ptr_in_sorted_list = ptr_in_list;
    } else {
        for (const auto& child : node->children) {
            new_update_scaled_frequency_for_leaf_nodes(child.second, sorted_path_scaled_frequency_space_tuples, frequency_counters);
        }
    }
}














void update_tree(TreeNode* root, const std::vector<std::pair<std::string, int>>& path_access_pairs) {
    for (std::vector<std::pair<std::string, int>>::const_iterator it = path_access_pairs.begin();
         it != path_access_pairs.end(); ++it) {
        const std::pair<std::string, int>& pair = *it;
        std::string path = pair.first;
        int weight = pair.second;
        std::istringstream iss(path);
        std::string part;
        TreeNode* current_node = root;
        std::string current_path;

        while (std::getline(iss, part, '/')) {
            if (!part.empty()) {
                current_path += "/" + part;
                // Qingxiu: check if the child exists
                bool found = false;
                auto it = current_node->children.find(current_path);
                if (it != current_node->children.end()) {
                    current_node = it->second;
                    found = true;
                }
                // for (auto& child : current_node->children) {
                //     if (child->name == current_path) {
                //         current_node = child;
                //         found = true;
                //         break;
                //     }
                // }
                if (!found) {
                    TreeNode* new_node = new TreeNode(current_path, 0, nullptr, 0.0, sorted_path_scaled_frequency_space_tuples.end());
                    current_node->add_child(new_node);
                    current_node = new_node;
                }
            }
        }
        current_node->weight = weight;
    }
}

std::vector<std::tuple<TreeNode*, std::string, int>> update_tree_for_eviction(TreeNode* root, const std::vector<std::pair<std::string, int>>& path_access_pairs) {
    std::vector<std::tuple<TreeNode*, std::string, int>> victims;
    // for (const auto& [path, weight] : path_access_pairs) {
    for (std::vector<std::pair<std::string, int>>::const_iterator it = path_access_pairs.begin();
         it != path_access_pairs.end(); ++it) {
        const std::pair<std::string, int>& pair = *it;
        std::string path = pair.first;
        int weight = pair.second;
        std::istringstream iss(path);
        std::string part;
        TreeNode* current_node = root;
        std::string current_path;

        while (std::getline(iss, part, '/')) {
            if (!part.empty()) {
                current_path += "/" + part;
                bool found = false;
                auto it = current_node->children.find(current_path);
                if (it != current_node->children.end()) {
                    current_node = it->second;
                    found = true;
                }
                // for (auto& child : current_node->children) {
                //     if (child->name == current_path) {
                //         current_node = child;
                //         found = true;
                //         break;
                //     }
                // }
                if (!found) {
                    TreeNode* new_node = new TreeNode(current_path, 0, nullptr, 0.0, sorted_path_scaled_frequency_space_tuples.end());
                    current_node->add_child(new_node);
                    current_node = new_node;
                }
            }
        }
        current_node->weight = weight;
        victims.push_back(std::make_tuple(current_node, current_path, weight));
    }
    return victims;
}

void remove_node(TreeNode* node_to_remove) {
    TreeNode* parent = node_to_remove->parent;
    if (parent) {
        auto it = parent->children.find(node_to_remove->name);
        if (it != parent->children.end()) {
            parent->children.erase(it);
        }
        // auto& siblings = parent->children;
        // auto iter = std::find(siblings.begin(), siblings.end(), node_to_remove);
        // if (iter != siblings.end()) {
        //     siblings.erase(iter);
        // }
    }
    delete node_to_remove;
}

void collect_descendants(TreeNode* node, std::vector<std::pair<std::string, std::list<std::tuple<TreeNode*, double, int>>::iterator>>& descendants) {
    descendants.push_back(std::make_pair(node->name, node->ptr_in_sorted_list));

    for (auto &child : node->children) {
        collect_descendants(child.second, descendants);
    }
}

std::vector<std::pair<std::string, std::list<std::tuple<TreeNode*, double, int>>::iterator>> get_all_descendants(TreeNode* node_to_remove) {
    std::vector<std::pair<std::string, std::list<std::tuple<TreeNode*, double, int>>::iterator>> descendants;
    collect_descendants(node_to_remove, descendants);

    return descendants;
}

// be careful to use this function because it incurs high overhead (traverse all tree) for large trees
// rm/rmdir/mv will use this function
TreeNode* find_node_by_name(TreeNode* root, const std::string& name) {
    if (root == nullptr) {
        return nullptr;
    }
    if (root->name == name) {
        return root;
    }
    for (auto &child : root->children) {
        TreeNode* result = find_node_by_name(child.second, name);
        if (result != nullptr) {
            return result;
        }
    }
    return nullptr;
}

void getLeafNodes(const TreeNode* node, std::vector<TreeNode*>& leaf_nodes) {
    if (!node)
        return;

    if (node->children.empty()) {
        leaf_nodes.push_back(const_cast<TreeNode*>(node));
    } else {
        for (const auto& child : node->children) {
            getLeafNodes(child.second, leaf_nodes);
        }
    }
}

void update_scaled_frequency_for_leaf_nodes(TreeNode* root, std::list<std::tuple<TreeNode*, double, int>>& sorted_path_scaled_frequency_space_tuples) {
    std::vector<TreeNode*> leaf_nodes;
    getLeafNodes(root, leaf_nodes);
    for (auto& leaf : leaf_nodes) {
        int free_space = 1;
        TreeNode* current = leaf;
        while (current->parent != nullptr) {
            if (current->parent->children.size() == 1) {
                free_space += 1;
                current = current->parent;
            } else {
                break;
            }
        }
        leaf->scaled_frequency = leaf->weight / static_cast<double>(free_space);

        auto it = std::find_if(
            sorted_path_scaled_frequency_space_tuples.begin(),
            sorted_path_scaled_frequency_space_tuples.end(),
            [&](const std::tuple<TreeNode*, double, int>& elem) {
                return std::get<0>(elem) == leaf;
            });

        if (it == sorted_path_scaled_frequency_space_tuples.end()) {
            auto ptr_in_list = sorted_path_scaled_frequency_space_tuples.emplace(
                sorted_path_scaled_frequency_space_tuples.end(), leaf, leaf->scaled_frequency, free_space);
            leaf->ptr_in_sorted_list = ptr_in_list;
        }
    }
    // Qingxiu: sort sorted_path_scaled_frequency_space_tuples by scaled frequency
    sorted_path_scaled_frequency_space_tuples.sort([](const std::tuple<TreeNode*, double, int>& a, const std::tuple<TreeNode*, double, int>& b) {
        return std::get<1>(a) < std::get<1>(b);
    });
}




TreeNode* copyTree(const TreeNode* root) {
    if (!root)
        return nullptr;

    TreeNode* new_node = new TreeNode(root->name, root->weight, nullptr, root->scaled_frequency, sorted_path_scaled_frequency_space_tuples.end());
    for (const auto& child : root->children) {
        TreeNode* new_child = copyTree(child.second);
        new_node->add_child(new_child);
    }

    return new_node;
}

void deleteTree(TreeNode* root) {
    if (!root)
        return;

    // Recursively delete all children
    for (auto& child : root->children) {
        deleteTree(child.second);
    }

    // Clear the children vector (optional, good practice)
    root->children.clear();

    // Delete the root node
    delete root;
}

#endif  // FSTREE_H