package btree;

import java.util.ArrayList;

/**
 * The abstract base class representing a node in a B-tree. Nodes can be either internal (index)
 * nodes or leaf nodes in a B-tree structure.
 */
public abstract class Node {
  /** The unique address of the node in the B-tree. */
  private Integer address;

  /** A flag indicating whether the node is a leaf node or an internal (index) node. */
  private Boolean isLeafNode;

  /**
   * Constructs a Node with the specified address and node type.
   *
   * @param address The unique address of the node in the B-tree.
   * @param isLeafNode A boolean flag indicating whether the node is a leaf node or an internal
   *     node.
   */
  public Node(int address, boolean isLeafNode) {
    this.address = address;
    this.isLeafNode = isLeafNode;
  }

  /**
   * Gets the address of the node.
   *
   * @return The unique address of the node in the B-tree.
   */
  public int getAddress() {
    return address;
  }

  /**
   * Checks if the node is a leaf node or an internal node.
   *
   * @return True if the node is a leaf node, false if it is an internal node.
   */
  public boolean isLeafNode() {
    return isLeafNode;
  }

  /**
   * Abstract method to get the size of the node in terms of storage space.
   *
   * @return The size of the node in bytes.
   */
  public abstract int getNodeSize();

  /**
   * Abstract method to get the smallest key in the node.
   *
   * @return The smallest key value stored in the node.
   */
  public abstract int getSmallestKey();

  /**
   * Abstract method to get the list of keys stored in the node.
   *
   * @return An ArrayList containing the keys stored in the node.
   */
  public abstract ArrayList<Integer> getKeys();

  /**
   * Abstract method to represent the node as a string.
   *
   * @return A string representation of the node.
   */
  public abstract String toString();
}
