package btree;

import common.Record;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

/**
 * Represents a leaf node in the B-tree. Leaf nodes store data entries consisting of keys and
 * associated records. Each leaf node contains a TreeMap to organize data entries in ascending order
 * of keys.
 */
public class LeafNode extends Node {
  private TreeMap<Integer, ArrayList<Record>> dataEntries;

  /**
   * Constructs a LeafNode with the specified address and leaf entries.
   *
   * @param address The address of the leaf node in the B-tree.
   * @param leafEntries The TreeMap containing data entries organized by key.
   */
  public LeafNode(int address, TreeMap<Integer, ArrayList<Record>> leafEntries) {
    super(address, true);
    this.dataEntries = leafEntries;
  }

  /**
   * Gets the number of data entries stored in the leaf node.
   *
   * @return The number of data entries in the leaf node.
   */
  public int getNodeSize() {
    return dataEntries.size();
  }

  /**
   * Gets the smallest key in the leaf node.
   *
   * @return The smallest key in the leaf node.
   */
  public int getSmallestKey() {
    return dataEntries.firstKey();
  }

  /**
   * Gets a list of keys stored in the leaf node.
   *
   * @return ArrayList containing all keys stored in the leaf node.
   */
  public ArrayList<Integer> getKeys() {
    return new ArrayList<Integer>(dataEntries.keySet());
  }

  /**
   * Returns a string representation of the leaf node, displaying its keys and associated records.
   *
   * @return String representation of the leaf node.
   */
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\nLeaf Node:");
    for (Map.Entry<Integer, ArrayList<Record>> entry : dataEntries.entrySet()) {
      sb.append("\nKey: " + entry.getKey() + ", Value: ");
      for (Record record : entry.getValue()) {
        sb.append(record.toString());
      }
    }
    return sb.toString();
  }

  /**
   * Gets the records associated with the specified key in the leaf node.
   *
   * @param key The key for which records are retrieved.
   * @return ArrayList of records associated with the given key, or null if the key is not found.
   */
  public ArrayList<Record> getRecords(int key) {
    return dataEntries.get(key);
  }
}
