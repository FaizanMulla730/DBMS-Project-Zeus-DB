package btree;

import common.Record;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import util.Constants;

/** BTreeSerializer contains methods to serialize a constructed index tree */
public class BTreeSerializer {
  private FileOutputStream fout;
  private FileChannel fc;
  private ByteBuffer buffer;

  /**
   * Constructs a BTreeSerializer
   *
   * @param indexFilePath the path to the output file for the serialized index tree
   */
  public BTreeSerializer(String indexFilePath) {
    initFileHandlers(indexFilePath);
    initHeader();
  }

  private void initFileHandlers(String indexFilePath) {
    try {
      this.fout = new FileOutputStream(indexFilePath);
      this.fc = fout.getChannel();
      this.buffer = ByteBuffer.allocate(Constants.IO.PAGE_SIZE);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /** Sets remaining values in the buffer to zero */
  private void setZeros() {
    while (buffer.hasRemaining()) {
      buffer.putInt(0);
    }
  }

  /** Initializes the header with zero values */
  private void initHeader() {
    try {
      setZeros();
      buffer.flip();
      fc.write(buffer);
      buffer.clear();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Serializes the header of the tree
   *
   * @param rootAddress the address of the root node
   * @param numLeaves the number of leaves in the tree
   * @param order the order of the tree
   */
  public void serializeHeader(int rootAddress, int numLeaves, int order) {
    try {
      // Address of the root
      buffer.putInt(rootAddress);
      // Number of leaves in the tree
      buffer.putInt(numLeaves);
      // Order of the tree
      buffer.putInt(order);
      setZeros();

      buffer.flip();
      fc.position(0);
      fc.write(buffer);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Serializes the given InternalNode
   *
   * @param internalNode the internal node to serialize
   */
  private void serializeInternalNode(InternalNode internalNode) {
    buffer.putInt(1); // Flag indicating it's an index node
    buffer.putInt(internalNode.getNodeSize()); // Number of keys in the node
    for (Integer key : internalNode.getKeys()) {
      buffer.putInt(key);
    }

    for (Node child : internalNode.getChildren()) {
      buffer.putInt(child.getAddress());
    }
  }

  /**
   * Serializes the given leafNode
   *
   * @param leafNode the leaf node to serialize
   */
  private void serializeLeafNode(LeafNode leafNode) {
    buffer.putInt(0); // Flag indicating it's a leaf node
    buffer.putInt(leafNode.getNodeSize()); // Number of keys in the node

    for (Integer key : leafNode.getKeys()) {
      buffer.putInt(key);
      ArrayList<Record> records = leafNode.getRecords(key);
      buffer.putInt(records.size()); // Number of records for the key
      for (Record record : records) {
        buffer.putInt(record.getPageId());
        buffer.putInt(record.getTupleId());
      }
    }
  }

  /**
   * Serialize all nodes in the given array of nodes
   *
   * @param nodes the array of nodes to serialize
   */
  public void serializeNodes(ArrayList<Node> nodes) {
    try {
      for (Node node : nodes) {
        if (node.isLeafNode()) {
          serializeLeafNode((LeafNode) node);
        } else {
          serializeInternalNode((InternalNode) node);
        }
        setZeros();
        buffer.flip();
        fc.write(buffer);
        buffer.clear();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /** Clears the buffer and closes the file channel and file output stream */
  public void close() {
    try {
      buffer.clear();
      fc.close();
      fout.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
