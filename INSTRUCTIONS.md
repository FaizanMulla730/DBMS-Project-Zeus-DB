# CS 5321/4321 Project Phase 2

## Top Level Class

The top level class is Compiler (src/main/java/compiler/Compiler.java).

```markdown
ZeusDB/src/main/java
├── btree
│   ├── BTreeDeserializer.java
│   ├── BTreeSerializer.java
│   ├── Btree.java
│   ├── InternalNode.java
│   ├── LeafNode.java
│   └── Node.java
├── common
│   ├── DBCatalog.java
│   ├── Index.java
│   ├── IndexBuilder.java
│   ├── QueryPlanBuilder.java
│   ├── Record.java
│   └── Tuple.java
├── compiler
│   └── Compiler.java
├── javaNIO
│   ├── Converter.java
│   ├── TupleReader.java
│   └── TupleWriter.java
├── logical
│   ├── LogicalDistinct.java
│   ├── LogicalJoin.java
│   ├── LogicalOperator.java
│   ├── LogicalProject.java
│   ├── LogicalScan.java
│   ├── LogicalSelect.java
│   └── LogicalSort.java
├── operator
│   ├── BlockNestedJoinOperator.java
│   ├── DuplicateEliminationOperator.java
│   ├── ExternalSortOperator.java
│   ├── IndexScanOperator.java
│   ├── JoinOperator.java
│   ├── Operator.java
│   ├── ProjectOperator.java
│   ├── ScanOperator.java
│   ├── SelectOperator.java
│   ├── SortMergeJoinOperator.java
│   └── SortOperator.java
├── util
│   ├── Config.java
│   ├── Constants.java
│   ├── Generator.java
│   ├── SortComparator.java
│   └── Tracker.java
└── visitor
    ├── AliasVisitor.java
    ├── ConditionVisitor.java
    ├── IndexVisitor.java
    ├── JoinConditionVisitor.java
    ├── JoinVisitor.java
    ├── PhysicalPlanBuilder.java
    ├── SelectVisitor.java
    └── SortMergeJoinVisitor.java


```

## Logic for extracting join conditions from WHERE clause

- To extract join conditions from the WHERE clause, we implemented a new JoinConditionVisitor to traverse the WHERE clause and determine how many columns are referenced in a given Expression and which tables and columns are referenced.
- To keep track of select conditions and the tables/columns they reference, we used a Map between table names and an array of Expressions that reference those tables.
- Similarly, for join conditions also we created a map between the outer table name and an array of Expressions that reference those tables.
- We then implemented a function to construct the join sub tree for a given join expression.
- The subtree is built using scan, and select operators for the tables referenced in the given join expression.
- At the root of this subtree is the join operator for the given expression.
- These join subtrees are then combined together using the left deep join method.
- The algorithm for creating the join tree can be broken down into the following steps:
  - Initialize Left and Right Subtrees:
    - Determine the left and right tables involved in the join operation based on the join condition.
    - Create the left subtree and right subtree independently. These subtrees represent individual tables or join conditions.
  - Combine Left and Right Subtrees:
    - Combine the schemas (list of columns) of the left and right subtrees. The combined schema represents the output schema of the join operation.
    - Create a new JoinOperator by specifying the combined schema, left subtree, right subtree, and the join condition expression.
  - Recursively combine the Join Subtrees:
    - For each table in the join order combine the previously evaluated subtree (if any) with the newly created subtree.
    - Continue this process until all tables in the join order are processed.

## Logic for handling partition reset during SMJ

To handle partition reset during external sort, we calculate the number of tuples that can fit on a page using the size of the tuples being sorted. Then, using the given index, we find the page it is located on using floor division, and reset the TupleReader to that page. Then, we read through that particular page until we get to the desired index. In this way, we avoid reading through every page in memory and keeping unbounded state.

## Logic for handling DISTINCT

Our implementation assumes the input is sorted (as suggested), so it does not keep unbounded state.

## Logic for separating select condition into index and non-index usable conditions

We created a new visitor, `IndexVisitor`, to process the select condition of a given LogicalSelect operator. We visit the expression, check if the column in the expression has an index, and, if so, set lowKey and highKey accordingly (using the setBounds() method). If not, the expression is added to a separate list of non-index usable conditions, which can then be converted into a single Expression to pass to a SelectOperator.

## Logic for B+ Tree traversal and handling clustered vs. unclustered indexes

We handle clustered and unclustered indexes in two places:

1. In the `IndexBuilder` class we sort and replace the relation file for clustered indexes.
2. In the `IndexScanOperator` the getNextTuple() method deserializes required nodes for fetching records in case of unclustered indexes. However in case of clustered indexes it just fetches the first record using deserialization and then uses the internal ScanOperator to fetch the subsequent tuples directly from the relation file.

## Logic for root-to-leaf tree descent

The BTreeDeserializer class performs the root-to-leaf descent on the Btree index and retrieves the first matching record from the leaf node.
This descent is initiated by extracting the root address from the index file header.
Subsequently, it locates a key in the root node that satisfies the given lowkey and highkey constraints, leading to a jump to the child associated with that key.
This descent process iterates until a leaf node is reached, wherein it identifies a key that is in the lowkey highkey interval. The corresponding record is then returned, and the pointers to this key and record are stored as class variables.
During subsequent record retrievals, the class utilizes these stored pointers to efficiently deserialize the leaf node and access the next record. This approach minimizes the need to deserialize the entire tree for each record retrieval, enhancing the overall efficiency of the B-tree traversal.

## Contributors

- Megh Khaire (mk2477)
- Tejas Lokhande (tjl244)
- Srinitya Allam (sa722)
- Gabriel Montalvo-Zotter(gmm243)
- Faizan Mulla (frm43)
