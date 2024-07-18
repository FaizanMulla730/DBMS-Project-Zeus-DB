package btree;

import common.DBCatalog;
import common.Index;
import common.Record;
import common.Tuple;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;
import net.sf.jsqlparser.schema.Column;
import operator.ScanOperator;

/** Btree represents a particular B+ index tree */
public class Btree {
  private Node root;
  private Integer order;
  private Column column;
  private Boolean isClustered;
  private Integer currentAddress;
  private ArrayList<Integer> keys;
  private BTreeSerializer serializer;

  /**
   * Constructs a Btree object
   *
   * @param index the index object containing information about the index the tree will be built on
   */
  public Btree(Index index) {
    this.root = null;
    this.order = index.getIndexOrder();
    this.column = index.getIndexColumn();
    this.isClustered = index.isClustered();
    this.currentAddress = 1;
    this.serializer = new BTreeSerializer(index.getIndexFilePath());
  }

  /**
   * Returns the root node of the tree
   *
   * @return the root node of the tree
   */
  public Node getRoot() {
    return root;
  }

  /**
   * Returns the order of the tree
   *
   * @return the order of the tree
   */
  public int getOrder() {
    return order;
  }

  /**
   * Returns whether the tree is clustered or not
   *
   * @return true if the tree is clustered, false if not
   */
  public boolean isClustered() {
    return isClustered;
  }

  /**
   * Sets the list of keys in the tree
   *
   * @param dataEntries the TreeMap representing the key, record pairs
   */
  private void setKeys(TreeMap<Integer, ArrayList<Record>> dataEntries) {
    this.keys = new ArrayList<Integer>(dataEntries.keySet());
  }

  /**
   * Returns the map of all key, record pairs in the tree
   *
   * @return the map of all key, record pairs in the tree
   */
  private TreeMap<Integer, ArrayList<Record>> getDataEntries() {
    TreeMap<Integer, ArrayList<Record>> dataEntries = new TreeMap<Integer, ArrayList<Record>>();
    String tableName = column.getTable().getName();
    ArrayList<Column> outputSchema = DBCatalog.getDB().getTableColumns(tableName);
    ScanOperator scanner = new ScanOperator(outputSchema);
    int indexColumnNum = scanner.getColumnNumberFromSchema(column);

    int pageId = 0;
    int tupleId = 0;
    ArrayList<Tuple> tuples;

    while ((tuples = scanner.getNextPage()) != null) {
      for (Tuple tuple : tuples) {
        int key = tuple.getElementAtIndex(indexColumnNum);

        if (dataEntries.containsKey(key)) {
          dataEntries.get(key).add(new Record(pageId, tupleId));
        } else {
          ArrayList<Record> records = new ArrayList<Record>();
          records.add(new Record(pageId, tupleId));
          dataEntries.put(key, records);
        }

        tupleId++;
      }
      pageId++;
      tupleId = 0;
    }
    return dataEntries;
  }

  /**
   * Creates a leaf node in the tree
   *
   * @param dataEntries the map of key, record pairs
   * @param size the intended size of the leaf node
   * @param start the index of the first key in the list of keys that should be in the leaf node
   * @return the new LeafNode
   */
  private LeafNode createLeafNode(
      TreeMap<Integer, ArrayList<Record>> dataEntries, int size, int start) {
    TreeMap<Integer, ArrayList<Record>> leafEntries = new TreeMap<Integer, ArrayList<Record>>();

    for (int i = start; i < start + size; i++) {
      int key = keys.get(i);
      leafEntries.put(key, dataEntries.get(key));
    }

    return new LeafNode(currentAddress, leafEntries);
  }

  /**
   * Returns the list of leaf nodes in the current leaf layer
   *
   * @param dataEntries the map of key, record pairs
   * @return the list of nodes in the current leaf layer
   */
  private ArrayList<Node> getLeafLayer(TreeMap<Integer, ArrayList<Record>> dataEntries) {
    ArrayList<Node> leafNodes = new ArrayList<Node>();
    int processedEntries = 0;
    int remainingEntries = dataEntries.size();

    while (remainingEntries >= order
        && !(remainingEntries > 2 * order && remainingEntries < 3 * order)) {
      int leafSize = Math.min(2 * order, remainingEntries);
      leafNodes.add(createLeafNode(dataEntries, leafSize, processedEntries));
      processedEntries += leafSize;
      remainingEntries -= leafSize;
      currentAddress++;
    }

    if (remainingEntries > 2 * order && remainingEntries < 3 * order) {
      int firstLeafEntries = remainingEntries / 2;
      leafNodes.add(createLeafNode(dataEntries, firstLeafEntries, processedEntries));
      processedEntries += firstLeafEntries;
      remainingEntries -= firstLeafEntries;
      currentAddress++;

      int secondLeafEntries = remainingEntries;
      leafNodes.add(createLeafNode(dataEntries, secondLeafEntries, processedEntries));
      processedEntries += secondLeafEntries;
      remainingEntries -= secondLeafEntries;
      currentAddress++;
    }

    return leafNodes;
  }

  /**
   * Creates a new internal tree node
   *
   * @param childNodes the list of nodes that should be this internal node's children
   * @param size the intended size of the leaf node
   * @param start the index of the key in the list of keys that should be in this node
   * @return the new InternalNode
   */
  private InternalNode createIndexNodes(ArrayList<Node> childNodes, int size, int start) {
    ArrayList<Integer> nodeKeys = new ArrayList<Integer>();
    ArrayList<Node> children = new ArrayList<Node>();

    for (int i = start; i < start + size; i++) {
      children.add(childNodes.get(i));
      if (i + 1 < start + size) {
        int smallestkey = childNodes.get(i + 1).getSmallestKey();
        nodeKeys.add(smallestkey);
      }
    }

    return new InternalNode(currentAddress, nodeKeys, children);
  }

  /**
   * Returns a list of internal nodes in the current layer of the tree
   *
   * @param childNodes the list of child nodes in the preceding layer of the tree
   * @return the list of nodes in the current layer
   */
  private ArrayList<Node> getIndexLayer(ArrayList<Node> childNodes) {
    ArrayList<Node> indexNodes = new ArrayList<Node>();
    int processedEntries = 0;
    int remainingEntries = childNodes.size();

    while (remainingEntries >= order
        && !(remainingEntries > 2 * order + 1 && remainingEntries < 3 * order + 2)) {
      int indexNodeSize = Math.min(remainingEntries, 2 * order + 1);
      indexNodes.add(createIndexNodes(childNodes, indexNodeSize, processedEntries));
      processedEntries += indexNodeSize;
      remainingEntries -= indexNodeSize;
      currentAddress++;
    }
    if (remainingEntries > 2 * order + 1 && remainingEntries < 3 * order + 2) {
      int firstNodeEntries = remainingEntries / 2;
      indexNodes.add(createIndexNodes(childNodes, firstNodeEntries, processedEntries));
      processedEntries += firstNodeEntries;
      remainingEntries -= firstNodeEntries;
      currentAddress++;

      int secondNodeEntries = remainingEntries;
      indexNodes.add(createIndexNodes(childNodes, secondNodeEntries, processedEntries));
      processedEntries += secondNodeEntries;
      remainingEntries -= secondNodeEntries;
      currentAddress++;
    } else if (remainingEntries > 0) {
      // Root node
      indexNodes.add(createIndexNodes(childNodes, remainingEntries, processedEntries));
      processedEntries += remainingEntries;
      remainingEntries = 0;
      currentAddress++;
    }

    return indexNodes;
  }

  /**
   * Sets the root of the tree to the given node
   *
   * @param node the node to set the root of the tree to
   */
  private void setRoot(Node node) {
    root = node;
  }

  /** Constructs and serializes the tree */
  public void constructAndSerialize() {
    // Create data entries
    TreeMap<Integer, ArrayList<Record>> dataEntries = getDataEntries();

    // Set keys for generating leaf layer
    setKeys(dataEntries);

    // Create leaf layer and serialize it:
    ArrayList<Node> leafNodes = getLeafLayer(dataEntries);
    serializer.serializeNodes(leafNodes);

    ArrayList<Node> indexNodes;
    // Handle special condition where there is only one leaf node
    if (leafNodes.size() == 1) {
      // Create an index node with no keys
      ArrayList<Integer> nodeKeys =
          new ArrayList<Integer>(); // Empty list, as no keys should be present
      ArrayList<Node> children = new ArrayList<Node>();
      children.add(leafNodes.get(0)); // Add the single leaf node as a child
      InternalNode indexNode = new InternalNode(currentAddress, nodeKeys, children);

      // Serialize the index node along with the leaf node
      indexNodes = new ArrayList<Node>();
      indexNodes.add(indexNode);
      serializer.serializeNodes(indexNodes);
      // Set root
      setRoot(indexNode);
    }

    // Create and serialize index layers:
    else {
      // Create the layer directly above the leaf layer:
      indexNodes = getIndexLayer(leafNodes);
      serializer.serializeNodes(indexNodes);
      // Create all other index layers till root:
      while (indexNodes.size() > 1) {
        indexNodes = getIndexLayer(indexNodes);
        serializer.serializeNodes(indexNodes);
      }
    }

    // Set root
    setRoot(indexNodes.get(0));
    // Serialize header page:
    serializer.serializeHeader(root.getAddress(), leafNodes.size(), order);
    serializer.close();
  }

  /**
   * Writes a string representation of the tree to the file indicated by the given file path
   *
   * @param filePath the path to the desired output file
   */
  public void dump(String filepath) {
    try {
      FileWriter myWriter = new FileWriter(filepath, true);
      myWriter.write(root.toString());
      myWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
