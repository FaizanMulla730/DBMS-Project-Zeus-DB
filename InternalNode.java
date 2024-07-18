package btree;

import java.util.ArrayList;

/**
 * Represents an internal node in a B-tree. Internal nodes contain keys and pointers to child nodes.
 * Each internal node acts as an index, facilitating efficient search and retrieval operations.
 */
public class InternalNode extends Node {
  private ArrayList<Integer> keys;
  private ArrayList<Node> children;

  /**
   * Constructs an InternalNode object with the specified address, keys, and child nodes.
   *
   * @param address The address of the internal node in the B-tree.
   * @param keys The list of keys stored in this internal node.
   * @param children The list of child nodes associated with each key in this internal node.
   */
  public InternalNode(int address, ArrayList<Integer> keys, ArrayList<Node> children) {
    super(address, false);
    this.keys = keys;
    this.children = children;
  }

  /**
   * Gets the number of keys stored in this internal node.
   *
   * @return The number of keys in this internal node.
   */
  public int getNodeSize() {
    return keys.size();
  }

  /**
   * Gets the smallest key value among all the child nodes of this internal node.
   *
   * @return The smallest key value in the child nodes.
   */
  public int getSmallestKey() {
    return children.get(0).getSmallestKey();
  }

  /**
   * Retrieves the list of keys stored in this internal node.
   *
   * @return The list of keys in this internal node.
   */
  public ArrayList<Integer> getKeys() {
    return keys;
  }

  /**
   * Generates a string representation of this internal node, including its keys and child nodes.
   *
   * @return A string representation of this internal node.
   */
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\nIndexNode with keys:");
    sb.append(keys);
    for (Node node : children) {
      sb.append(node.toString());
    }
    return sb.toString();
  }

  /**
   * Retrieves the list of child nodes associated with each key in this internal node.
   *
   * @return The list of child nodes in this internal node.
   */
  public ArrayList<Node> getChildren() {
    return children;
  }

  /**
   * Retrieves the child node associated with the specified key in this internal node.
   *
   * @param key The key for which the child node is retrieved.
   * @return The child node associated with the specified key.
   */
  public Node getChild(int key) {
    return children.get(key);
  }
}
