package btree;

import common.Index;
import common.Record;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import util.Constants;

/** BTreeDeserializer contains methods to deserialize the tree for IndexScanOperators */
public class BTreeDeserializer {
  private FileInputStream fin;
  private FileChannel fc;
  private ByteBuffer buffer;
  private Integer currentAddress;
  private Integer rootAddress;
  private Integer previousRecordIndex;
  private Integer previousKeyIndex;
  private Integer numLeaves;

  /**
   * Constructs a BTreeDeserializer
   *
   * @param index the index object containing information about the index the tree was constructed
   *     on
   */
  public BTreeDeserializer(Index index) {
    initFileHandlers(index);
    this.currentAddress = 0;
    this.previousKeyIndex = 0;
    this.previousRecordIndex = null;

    deserializeHeader();
  }

  private void initFileHandlers(Index index) {
    try {
      this.fin = new FileInputStream(index.getIndexFilePath());
      this.fc = fin.getChannel();
      this.buffer = ByteBuffer.allocate(Constants.IO.PAGE_SIZE);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /** Reads the current address from the internal buffer */
  private void readBufferForCurrentAddress() {
    try {
      buffer.clear();
      fc.position(currentAddress * Constants.IO.PAGE_SIZE);
      fc.read(buffer);
      buffer.flip();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /** Deserializes the root node of the tree */
  private void deserializeHeader() {
    readBufferForCurrentAddress();

    rootAddress = buffer.getInt();
    numLeaves = buffer.getInt();
    buffer.getInt();
    currentAddress = rootAddress;
  }

  /**
   * Increments the current buffer position until the desired key is reached
   *
   * @param from the starting key
   * @param to the destination key
   */
  private void jumpNodeKeys(int from, int to) {
    while (from < to) {
      buffer.getInt();
      from++;
    }
  }

  /**
   * Deserializes all of the internal nodes along the path to the first leaf node with a key in the
   * interval [lowkey, highkey]
   *
   * @param lowkey the lower key bound
   * @param highkey the upper key bound
   * @return the record of the first leaf node with key >= lowkey
   */
  private Record deserializeInternalNodes(int lowkey, int highkey) {
    int keyIndex;
    int size = buffer.getInt();
    for (keyIndex = 0; keyIndex < size; keyIndex++) {
      int temp = buffer.getInt();
      if (temp >= lowkey) {
        break;
      }
    }
    // Skip all remaining keys:
    jumpNodeKeys(keyIndex + 1, size);

    for (int i = 0; i < size + 1; i++) {
      int childAddress = buffer.getInt();

      if (i == keyIndex) {
        currentAddress = childAddress;
        break;
      }
    }
    jumpNodeKeys(keyIndex + 1, size + 1);

    return deserializeNodes(lowkey, highkey);
  }

  /**
   * Deserializes a single internal node and returns the next node in the layer below with key in
   * interval [lowkey, highkey]
   *
   * @param lowkey the lower bound
   * @param highkey the upper bound
   * @return the record of the first node in the layer below with a key in the interval [lowkey,
   *     highkey]
   */
  private Record deserializeInternalNode(int lowkey, int highkey) {
    Record record = null;
    int size = buffer.getInt();

    for (int i = 0; i < size; i++) {
      int key = buffer.getInt();
      int numRecords = buffer.getInt();

      for (int j = 0; j < numRecords; j++) {
        int pageId = buffer.getInt();
        int tupleId = buffer.getInt();

        if (key >= lowkey && key <= highkey) {
          previousKeyIndex = i;
          previousRecordIndex = j;
          return new Record(pageId, tupleId);
        } else if (key > highkey) {
          return null;
        }
      }
    }

    return record;
  }

  /**
   * Traverses all nodes along the path to the first leaf node with a key in the interval [lowkey,
   * highkey]
   *
   * @param lowkey the lower bound
   * @param highkey the upper bound
   * @return the record of the first leaf node with key in the interval [lowkey, highkey]
   */
  private Record deserializeNodes(int lowkey, int highkey) {
    readBufferForCurrentAddress();

    int nodeType = buffer.getInt();

    if (nodeType == 0) {
      // After reaching the root node, return the first record that matches
      // the given lowkey and highkey condition.
      return deserializeInternalNode(lowkey, highkey);
    } else if (nodeType == 1) {
      // Traverse all the index node and try to reach the root node
      return deserializeInternalNodes(lowkey, highkey);
    }

    close();
    return null;
  }

  /**
   * Returns the record of the first leaf node with key in the interval [lowkey, highkey]
   *
   * @param lowkey the lower bound
   * @param highkey the upper bound
   * @return the record of the first leaf node with key in the interval [lowkey, highkey]
   */
  public Record findFirstRecord(int lowkey, int highkey) {
    return deserializeNodes(lowkey, highkey);
  }

  /**
   * Returns the record following the previously returned record for the same key
   *
   * @return the next record for the previous key
   */
  private Record getNextKeyRecord() {
    for (int i = 0; i <= previousKeyIndex; i++) {
      buffer.getInt();
      int numRecords = buffer.getInt();

      for (int j = 0; j < numRecords; j++) {
        int pageId = buffer.getInt();
        int tupleId = buffer.getInt();

        if (i == previousKeyIndex && (previousRecordIndex == null || j > previousRecordIndex)) {
          previousRecordIndex = j;
          return new Record(pageId, tupleId);
        }
      }
    }
    return null;
  }

  /**
   * Returns the next record in the interval [lowkey, highkey]
   *
   * @param lowkey the lower bound
   * @param highkey the upper bound
   * @return the next record in the given interval
   */
  public Record getNextRecord(int lowkey, int highkey) {
    while (true) {
      // Read the buffer at the current address
      readBufferForCurrentAddress();

      // If the current address is not pointing to a leaf node return null
      if (buffer.getInt() != 0) return null;

      // Get size of the leaf node
      int size = buffer.getInt();

      // Check if any other record exists for the previous key
      Record record = getNextKeyRecord();
      if (record != null) return record;
      else previousRecordIndex = null;

      for (int i = previousKeyIndex + 1; i < size; i++) {
        int key = buffer.getInt();
        int numRecords = buffer.getInt();

        for (int j = 0; j < numRecords; j++) {
          int pageId = buffer.getInt();
          int tupleId = buffer.getInt();
          if (key >= lowkey && key <= highkey) {
            previousKeyIndex = i;
            previousRecordIndex = j;
            return new Record(pageId, tupleId);
          } else if (key > highkey) {
            return null;
          }
        }
      }

      // Move to the next leaf node
      currentAddress++;
      previousKeyIndex = 0;
      previousRecordIndex = null;
    }
  }

  public Integer getNumLeaves() {
    deserializeHeader();
    return numLeaves;
  }

  /** Closes the buffer, file channel, and file input stream */
  public void close() {
    try {
      buffer.clear();
      fc.close();
      fin.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void reset(Index index) {
    try {
      this.fin = new FileInputStream(index.getIndexFilePath());
      this.fc = fin.getChannel();
      this.currentAddress = 0;
      this.previousKeyIndex = 0;
      this.previousRecordIndex = null;
      deserializeHeader();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
