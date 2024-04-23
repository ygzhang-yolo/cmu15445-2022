//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_trie.h
//
// Identification: src/include/primer/p0_trie.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stack>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rwlatch.h"

namespace bustub {

/**
 * TrieNode is a generic container for any node in Trie.
 */
class TrieNode {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie Node object with the given key char.
   * is_end_ flag should be initialized to false in this constructor.
   *
   * @param key_char Key character of this trie node
   */
  explicit TrieNode(char key_char) {
    key_char_ = key_char;
    is_end_ = false;
    children_.clear();
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Move constructor for trie node object. The unique pointers stored
   * in children_ should be moved from other_trie_node to new trie node.
   *
   * @param other_trie_node Old trie node.
   */
  TrieNode(TrieNode &&other_trie_node) noexcept {
    key_char_ = other_trie_node.key_char_;
    is_end_ = other_trie_node.is_end_;
    children_.swap(other_trie_node.children_);  // tips: 用右值移动的方式来构造
  }

  /**
   * @brief Destroy the TrieNode object.
   */
  virtual ~TrieNode() = default;

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has a child node with specified key char.
   *
   * @param key_char Key char of child node.
   * @return True if this trie node has a child with given key, false otherwise.
   */
  bool HasChild(char key_char) const { return children_.find(key_char) != children_.end(); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has any children at all. This is useful
   * when implementing 'Remove' functionality.
   *
   * @return True if this trie node has any child node, false if it has no child node.
   */
  bool HasChildren() const { return !children_.empty(); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node is the ending character of a key string.
   *
   * @return True if is_end_ flag is true, false if is_end_ is false.
   */
  bool IsEndNode() const { return is_end_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Return key char of this trie node.
   *
   * @return key_char_ of this trie node.
   */
  char GetKeyChar() const { return key_char_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert a child node for this trie node into children_ map, given the key char and
   * unique_ptr of the child node. If specified key_char already exists in children_,
   * return nullptr. If parameter `child`'s key char is different than parameter
   * `key_char`, return nullptr.
   *
   * Note that parameter `child` is rvalue and should be moved when it is
   * inserted into children_map.
   *
   * The return value is a pointer to unique_ptr because pointer to unique_ptr can access the
   * underlying data without taking ownership of the unique_ptr. Further, we can set the return
   * value to nullptr when error occurs.
   *
   * @param key Key of child node
   * @param child Unique pointer created for the child node. This should be added to children_ map.
   * @return Pointer to unique_ptr of the inserted child node. If insertion fails, return nullptr.
   */
  std::unique_ptr<TrieNode> *InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child) {
    // 遍历找插入位置, 插入失败返回null
    if (HasChild(key_char) || key_char != child->key_char_) {
      return nullptr;
    }
    children_[key_char] = std::forward<std::unique_ptr<TrieNode>>(child);
    return &children_[key_char];
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the child node given its key char. If child node for given key char does
   * not exist, return nullptr.
   *
   * @param key Key of child node
   * @return Pointer to unique_ptr of the child node, nullptr if child
   *         node does not exist.
   */
  std::unique_ptr<TrieNode> *GetChildNode(char key_char) {
    auto node = children_.find(key_char);
    if (node != children_.end()) {
      return &(node->second);
    }
    return nullptr;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove child node from children_ map.
   * If key_char does not exist in children_, return immediately.
   *
   * @param key_char Key char of child node to be removed
   */
  void RemoveChildNode(char key_char) {
    auto node = children_.find(key_char);
    if (node != children_.end()) {
      children_.erase(key_char);
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Set the is_end_ flag to true or false.
   *
   * @param is_end Whether this trie node is ending char of a key string
   */
  void SetEndNode(bool is_end) { is_end_ = is_end; }

 protected:
  /** Key character of this trie node */
  char key_char_;
  /** whether this node marks the end of a key */
  bool is_end_{false};
  /** A map of all child nodes of this trie node, which can be accessed by each
   * child node's key char. */
  std::unordered_map<char, std::unique_ptr<TrieNode>> children_;
};

/**
 * TrieNodeWithValue is a node that marks the ending of a key, and it can
 * hold a value of any type T.
 */
template <typename T>
class TrieNodeWithValue : public TrieNode {
 private:
  /* Value held by this trie node. */
  T value_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue object from a TrieNode object and specify its value.
   * This is used when a non-terminal TrieNode is converted to terminal TrieNodeWithValue.
   *
   * The children_ map of TrieNode should be moved to the new TrieNodeWithValue object.
   * Since it contains unique pointers, the first parameter is a rvalue reference.
   *
   * You should:
   * 1) invoke TrieNode's move constructor to move data from TrieNode to
   * TrieNodeWithValue.
   * 2) set value_ member variable of this node to parameter `value`.
   * 3) set is_end_ to true
   *
   * @param trieNode TrieNode whose data is to be moved to TrieNodeWithValue
   * @param value
   */
  TrieNodeWithValue(TrieNode &&trieNode, T value) : TrieNode(std::forward<TrieNode>(trieNode)) {
    value_ = value;
    SetEndNode(true);
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue. This is used when a new terminal node is constructed.
   *
   * You should:
   * 1) Invoke the constructor for TrieNode with the given key_char.
   * 2) Set value_ for this node.
   * 3) set is_end_ to true.
   *
   * @param key_char Key char of this node
   * @param value Value of this node
   */
  TrieNodeWithValue(char key_char, T value) : TrieNode(key_char) {
    value_ = value;
    SetEndNode(true);
  }

  /**
   * @brief Destroy the Trie Node With Value object
   */
  ~TrieNodeWithValue() override = default;

  /**
   * @brief Get the stored value_.
   *
   * @return Value of type T stored in this node
   */
  T GetValue() const { return value_; }
};

/**
 * Trie is a concurrent key-value store. Each key is a string and its corresponding
 * value can be any type.
 */
class Trie {
 private:
  /* Root node of the trie */
  std::unique_ptr<TrieNode> root_;
  /* Read-write lock for the trie */
  ReaderWriterLatch latch_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie object. Initialize the root node with '\0'
   * character.
   */
  Trie() {
    auto *root = new TrieNode('\0');
    root_.reset(root);
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert key-value pair into the trie.
   *
   * If the key is an empty string, return false immediately.
   *
   * If the key already exists, return false. Duplicated keys are not allowed and
   * you should never overwrite value of an existing key.
   *
   * When you reach the ending character of a key:
   * 1. If TrieNode with this ending character does not exist, create new TrieNodeWithValue
   * and add it to parent node's children_ map.
   * 2. If the terminal node is a TrieNode, then convert it into TrieNodeWithValue by
   * invoking the appropriate constructor.
   * 3. If it is already a TrieNodeWithValue,
   * then insertion fails and returns false. Do not overwrite existing data with new data.
   *
   * You can quickly check whether a TrieNode pointer holds TrieNode or TrieNodeWithValue
   * by checking the is_end_ flag. If is_end_ == false, then it points to TrieNode. If
   * is_end_ == true, it points to TrieNodeWithValue.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param value Value to be inserted
   * @return True if insertion succeeds, false if the key already exists
   */
  template <typename T>
  bool Insert(const std::string &key, T value) {
    // 特判key == null
    if (key.empty()) {
      return false;
    }
    this->latch_.WLock();
    // 遍历整个树, 如果每层存在对应字符，则遍历
    auto tree = &this->root_;
    for (auto c : key) {
      if (tree->get()->HasChild(c)) {  // tips: unique_ptr用get方法获取原始指针
        // 如果每层存在对应字符，则遍历下一层
        tree = tree->get()->GetChildNode(c);
      } else {
        // 如果每层不存在对应字符，则创建一个新的
        tree = tree->get()->InsertChildNode(c, std::make_unique<TrieNode>(c));
      }
    }
    // case1: 如果已存在此节点，返回false;
    if (tree->get()->IsEndNode()) {
      this->latch_.WUnlock();
      return false;
    }
    // case2: 不存在此节点, 则创建一个TrieNodeWithValue, 一个运行时多态
    auto v = new TrieNodeWithValue(std::move(*(tree->get())),
                                   value);  // tips: 必须要将原节点移动过来，因为他还有对应的children;
    tree->reset(v);                         // tips: unique_ptr的reset方法用来替换原始指针
    this->latch_.WUnlock();
    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove key value pair from the trie.
   * This function should also remove nodes that are no longer part of another
   * key. If key is empty or not found, return false.
   *
   * You should:
   * 1) Find the terminal node for the given key.
   * 2) If this terminal node does not have any children, remove it from its
   * parent's children_ map.
   * 3) Recursively remove nodes that have no children and are not terminal node
   * of another key.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @return True if the key exists and is removed, false otherwise
   */
  bool Remove(const std::string &key) {
    // 特判 key为空
    if (key.empty()) {
      return false;
    }
    this->latch_.WLock();
    // 1. 根据给定的键遍历 trie。如果密钥不存在，请立即返回
    auto tree = &this->root_;
    std::vector<std::unique_ptr<TrieNode> *> keyNodes;  //用来存储遍历的node路径, 因为后面需要反向遍历
    for (auto c : key) {
      auto next = tree->get()->GetChildNode(c);
      if (next == nullptr) {
        // 如果未找到对应的key, 返回错误
        this->latch_.WUnlock();
        return false;
      }
      keyNodes.emplace_back(tree);
      tree = next;
    }
    // 2. 将is_end设置为false;
    tree->get()->SetEndNode(false);
    // 3. 如果此节点没有子节点, 将他从父节点的映射中删除, 并递归删除所有没有子节点的节点
    for (int i = keyNodes.size() - 1; i >= 0; --i) {
      auto pre = keyNodes[i];  // pre恰好是最后一个字符的前一个指向
      if (!tree->get()->IsEndNode() || tree->get()->HasChildren()) {
        // end conditon: 要删除的节点是end，或有其他的children, 停止遍历
        this->latch_.WUnlock();
        return true;
      }
      pre->get()->RemoveChildNode(key[i]);  //这个对应关系，梳理下上面的过程很容易得知
      tree = pre;
    }
    this->latch_.WUnlock();
    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the corresponding value of type T given its key.
   * If key is empty, set success to false.
   * If key does not exist in trie, set success to false.
   * If the given type T is not the same as the value type stored in TrieNodeWithValue
   * (ie. GetValue<int> is called but terminal node holds std::string),
   * set success to false.
   *
   * To check whether the two types are the same, dynamic_cast
   * the terminal TrieNode to TrieNodeWithValue<T>. If the casted result
   * is not nullptr, then type T is the correct type.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param success Whether GetValue is successful or not
   * @return Value of type T if type matches
   */

  template <typename T>
  T GetValue(const std::string &key, bool *success) {
    // 特判: key为空
    if (key.empty()) {
      *success = false;  // tips: 通过指针模拟多返回值
      return {};
    }
    latch_.RLock();
    // 遍历整个tree来查找
    auto tree = &this->root_;
    for (auto c : key) {
      auto next = tree->get()->GetChildNode(c);
      if (next == nullptr) {
        // 1. 如果查不到对应的key, 返回 null + false;
        *success = false;
        latch_.RUnlock();
        return {};
      }
      tree = next;
    }
    // 2. 如果查到的终点, 不是End返回null + false;
    if (!tree->get()->IsEndNode()) {
      *success = false;
      latch_.RUnlock();
      return {};
    }
    // 3. 如果是终点, 返回一个结果
    auto end_node = dynamic_cast<TrieNodeWithValue<T> *>(tree->get());
    // tips: 一个安全性检查, 按理说, 终点都是TrieNodeWithValue类型的;
    if (end_node) {
      *success = true;
      latch_.RUnlock();
      return end_node->GetValue();
    }
    *success = false;
    latch_.RUnlock();
    return {};
  }
};
}  // namespace bustub
