package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.table.RecordId;

import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

/**
 * A persistent B+ tree.
 *
 *   BPlusTree tree = new BPlusTree(bufferManager, metadata, lockContext);
 *
 *   // Insert some values into the tree.
 *   tree.put(new IntDataBox(0), new RecordId(0, (short) 0));
 *   tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
 *   tree.put(new IntDataBox(2), new RecordId(2, (short) 2));
 *
 *   // Get some values out of the tree.
 *   tree.get(new IntDataBox(0)); // Optional.of(RecordId(0, 0))
 *   tree.get(new IntDataBox(1)); // Optional.of(RecordId(1, 1))
 *   tree.get(new IntDataBox(2)); // Optional.of(RecordId(2, 2))
 *   tree.get(new IntDataBox(3)); // Optional.empty();
 *
 *   // Iterate over the record ids in the tree.
 *   tree.scanEqual(new IntDataBox(2));        // [(2, 2)]
 *   tree.scanAll();                             // [(0, 0), (1, 1), (2, 2)]
 *   tree.scanGreaterEqual(new IntDataBox(1)); // [(1, 1), (2, 2)]
 *
 *   // Remove some elements from the tree.
 *   tree.get(new IntDataBox(0)); // Optional.of(RecordId(0, 0))
 *   tree.remove(new IntDataBox(0));
 *   tree.get(new IntDataBox(0)); // Optional.empty()
 *
 *   // Load the tree (same as creating a new tree).
 *   BPlusTree fromDisk = new BPlusTree(bufferManager, metadata, lockContext);
 *
 *   // All the values are still there.
 *   fromDisk.get(new IntDataBox(0)); // Optional.empty()
 *   fromDisk.get(new IntDataBox(1)); // Optional.of(RecordId(1, 1))
 *   fromDisk.get(new IntDataBox(2)); // Optional.of(RecordId(2, 2))
 */
public class BPlusTree {
    // Buffer manager
    private BufferManager bufferManager;

    // B+ tree metadata
    private BPlusTreeMetadata metadata;

    // root of the B+ tree
    private BPlusNode root;

    // lock context for the B+ tree
    private LockContext lockContext;

    // Constructors ////////////////////////////////////////////////////////////
    /**
     * Construct a new B+ tree with metadata `metadata` and lock context `lockContext`.
     * `metadata` contains information about the order, partition number,
     * root page number, and type of keys.
     *
     * If the specified order is so large that a single node cannot fit on a
     * single page, then a BPlusTree exception is thrown. If you want to have
     * maximally full B+ tree nodes, then use the BPlusTree.maxOrder function
     * to get the appropriate order.
     *
     * We additionally write a row to the _metadata.indices table with metadata about
     * the B+ tree:
     *
     *   - the name of the tree (table associated with it and column it indexes)
     *   - the key schema of the tree,
     *   - the order of the tree,
     *   - the partition number of the tree,
     *   - the page number of the root of the tree.
     *
     * All pages allocated on the given partition are serializations of inner and leaf nodes.
     */
    public BPlusTree(BufferManager bufferManager, BPlusTreeMetadata metadata, LockContext lockContext) {
        // Prevent child locks - we only lock the entire tree as a whole.
        lockContext.disableChildLocks();
        // By default we want to read the whole tree
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.S);

        // Sanity checks.
        if (metadata.getOrder() < 0) {
            String msg = String.format(
                    "You cannot construct a B+ tree with negative order %d.",
                    metadata.getOrder());
            throw new BPlusTreeException(msg);
        }

        int maxOrder = BPlusTree.maxOrder(BufferManager.EFFECTIVE_PAGE_SIZE, metadata.getKeySchema());
        if (metadata.getOrder() > maxOrder) {
            String msg = String.format(
                    "You cannot construct a B+ tree with order %d greater than the " +
                            "max order %d.",
                    metadata.getOrder(), maxOrder);
            throw new BPlusTreeException(msg);
        }

        this.bufferManager = bufferManager;
        this.lockContext = lockContext;
        this.metadata = metadata;

        if (this.metadata.getRootPageNum() != DiskSpaceManager.INVALID_PAGE_NUM) {
            this.root = BPlusNode.fromBytes(this.metadata, bufferManager, lockContext,
                    this.metadata.getRootPageNum());
        } else {
            // We're creating the root, which means we need exclusive access
            // on the tree
            LockUtil.ensureSufficientLockHeld(lockContext, LockType.X);
            // Construct the root.
            List<DataBox> keys = new ArrayList<>();
            List<RecordId> rids = new ArrayList<>();
            Optional<Long> rightSibling = Optional.empty();
            this.updateRoot(new LeafNode(this.metadata, bufferManager, keys, rids, rightSibling, lockContext));
        }
    }

    // Core API ////////////////////////////////////////////////////////////////
    /**
     * Returns the value associated with `key`.
     *
     *   // Insert a single value into the tree.
     *   DataBox key = new IntDataBox(42);
     *   RecordId rid = new RecordId(0, (short) 0);
     *   tree.put(key, rid);
     *
     *   // Get the value we put and also try to get a value we never put.
     *   tree.get(key);                 // Optional.of(rid)
     *   tree.get(new IntDataBox(100)); // Optional.empty()
     */
    public Optional<RecordId> get(DataBox key) {
        typecheck(key);
        // (proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.S);

        // (proj2): implement
        // BPlusTree类中的get方法发起调用，InnerNode中的get是recursive case
        // LeafNode中的get是base case
        // root是一个LeafNode，其get()函数返回一个LeafNode或null？
        // LeafNode可以使用getKey方法取得key对应的RecordId，返回格式为Optional<RecordId>
        return root.get(key).getKey(key);
    }

    /**
     * scanEqual(k) is equivalent to get(k) except that it returns an iterator
     * instead of an Optional. That is, if get(k) returns Optional.empty(),
     * then scanEqual(k) returns an empty iterator. If get(k) returns
     * Optional.of(rid) for some rid, then scanEqual(k) returns an iterator
     * over rid.
     */
    public Iterator<RecordId> scanEqual(DataBox key) {
        typecheck(key);
        // (proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.S);

        Optional<RecordId> rid = get(key);
        if (rid.isPresent()) {
            ArrayList<RecordId> l = new ArrayList<>();
            l.add(rid.get());
            return l.iterator();
        } else {
            return Collections.emptyIterator();
        }
    }

    /**
     * Returns an iterator over all the RecordIds stored in the B+ tree in
     * ascending order of their corresponding keys.
     *
     *   // Create a B+ tree and insert some values into it.
     *   BPlusTree tree = new BPlusTree("t.txt", Type.intType(), 4);
     *   tree.put(new IntDataBox(2), new RecordId(2, (short) 2));
     *   tree.put(new IntDataBox(5), new RecordId(5, (short) 5));
     *   tree.put(new IntDataBox(4), new RecordId(4, (short) 4));
     *   tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
     *   tree.put(new IntDataBox(3), new RecordId(3, (short) 3));
     *
     *   Iterator<RecordId> iter = tree.scanAll();
     *   iter.next(); // RecordId(1, 1)
     *   iter.next(); // RecordId(2, 2)
     *   iter.next(); // RecordId(3, 3)
     *   iter.next(); // RecordId(4, 4)
     *   iter.next(); // RecordId(5, 5)
     *   iter.next(); // NoSuchElementException
     *
     * Note that you CAN NOT materialize all record ids in memory and then
     * return an iterator over them. Your iterator must lazily scan over the
     * leaves of the B+ tree. Solutions that materialize all record ids in
     * memory will receive 0 points.
     */
    public Iterator<RecordId> scanAll() {
        // (proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.S);

        // (proj2): Return a BPlusTreeIterator.

        return new BPlusTreeIterator();
    }

    /**
     * Returns an iterator over all the RecordIds stored in the B+ tree that
     * are greater than or equal to `key`. RecordIds are returned in ascending
     * of their corresponding keys.
     *
     *   // Insert some values into a tree.
     *   tree.put(new IntDataBox(2), new RecordId(2, (short) 2));
     *   tree.put(new IntDataBox(5), new RecordId(5, (short) 5));
     *   tree.put(new IntDataBox(4), new RecordId(4, (short) 4));
     *   tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
     *   tree.put(new IntDataBox(3), new RecordId(3, (short) 3));
     *
     *   Iterator<RecordId> iter = tree.scanGreaterEqual(new IntDataBox(3));
     *   iter.next(); // RecordId(3, 3)
     *   iter.next(); // RecordId(4, 4)
     *   iter.next(); // RecordId(5, 5)
     *   iter.next(); // NoSuchElementException
     *
     * Note that you CAN NOT materialize all record ids in memory and then
     * return an iterator over them. Your iterator must lazily scan over the
     * leaves of the B+ tree. Solutions that materialize all record ids in
     * memory will receive 0 points.
     */
    public Iterator<RecordId> scanGreaterEqual(DataBox key) {
        typecheck(key);
        // (proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.S);

        // (proj2): Return a BPlusTreeIterator.

        return new BPlusTreeIterator(key);
    }

    /**
     * Inserts a (key, rid) pair into a B+ tree. If the key already exists in
     * the B+ tree, then the pair is not inserted and an exception is raised.
     *
     *   DataBox key = new IntDataBox(42);
     *   RecordId rid = new RecordId(42, (short) 42);
     *   tree.put(key, rid); // Success :)
     *   tree.put(key, rid); // BPlusTreeException :(
     */
    public void put(DataBox key, RecordId rid) {
        typecheck(key);
        // (proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.X);

        // (proj2): implement
        // Note: You should NOT update the root variable directly.
        // Use the provided updateRoot() helper method to change
        // the tree's root if the old root splits.

        Optional<Pair<DataBox, Long>> o = root.put(key, rid);
        // 如果不需要分裂就直接返回
        if (!o.isPresent()) {
            return;
        }

        // 下面的分裂逻辑
        Pair<DataBox, Long> p = o.get();
        List<DataBox> keys = new ArrayList<>();
        keys.add(p.getFirst()); // 将传上来的key设置为新根节点的第一个key
        List<Long> children = new ArrayList<>();
        children.add(root.getPage().getPageNum());  // 原来的根节点是新根节点的左孩子
        children.add(p.getSecond());
        updateRoot(new InnerNode(metadata, bufferManager, keys, children, lockContext));    // 根节点是InnerNode
    }

    /**
     * Bulk loads data into the B+ tree. Tree should be empty and the data
     * iterator should be in sorted order (by the DataBox key field) and
     * contain no duplicates (no error checking is done for this).
     *
     * fillFactor specifies the fill factor for leaves only; inner nodes should
     * be filled up to full and split in half exactly like in put.
     *
     * This method should raise an exception if the tree is not empty at time
     * of bulk loading. If data does not meet the preconditions (contains
     * duplicates or not in order), the resulting behavior is undefined.
     * Undefined behavior means you can handle these cases however you want
     * (or not at all) and you are not required to write any explicit checks.
     *
     * The behavior of this method should be similar to that of InnerNode's
     * bulkLoad (see comments in BPlusNode.bulkLoad).
     */
    public void bulkLoad(Iterator<Pair<DataBox, RecordId>> data, float fillFactor) {
        // (proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.X);

        // (proj2): implement
        // Note: You should NOT update the root variable directly.
        // Use the provided updateRoot() helper method to change
        // the tree's root if the old root splits.
        // 获得叶子结点来判断是否为空树
        LeafNode left = this.root.getLeftmostLeaf();
        if (left != this.root // 叶子不等于根 证明有数据
                || left.scanAll().hasNext()) { // 叶子下的迭代器能后进行hasNext 证明有数据
            throw new BPlusTreeException("cannot bulk load into nonempty tree");
        }
        // 传进来是一个Iterator,在前面没有想到要在开始的时候去迭代它
        while (data.hasNext()) {
            // 递归往下走
            Optional<Pair<DataBox, Long>> optional = this.root.bulkLoad(data, fillFactor);
            if (optional.isPresent()) {
                Pair<DataBox, Long> p = optional.get();

                // 组装新root节点的keys和children
                List<DataBox> keys = new ArrayList<>();
                keys.add(p.getFirst());

                List<Long> children = new ArrayList<>();
                children.add(root.getPage().getPageNum());
                children.add(p.getSecond());

                // 使用官方推荐的updateRoot来更新结点,不要用直接赋值的方式
                updateRoot(new InnerNode(metadata, bufferManager, keys, children, lockContext));
            }
        }
    }

    /**
     * Deletes a (key, rid) pair from a B+ tree.
     *
     *   DataBox key = new IntDataBox(42);
     *   RecordId rid = new RecordId(42, (short) 42);
     *
     *   tree.put(key, rid);
     *   tree.get(key); // Optional.of(rid)
     *   tree.remove(key);
     *   tree.get(key); // Optional.empty()
     */
    public void remove(DataBox key) {
        typecheck(key);
        // (proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.X);

        // (proj2): implement

        this.root.remove(key);
    }

    // Helpers /////////////////////////////////////////////////////////////////
    /**
     * Returns a sexp representation of this tree. See BPlusNode.toSexp for
     * more information.
     */
    public String toSexp() {
        // (proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.S);
        return root.toSexp();
    }

    /**
     * Debugging large B+ trees is hard. To make it a bit easier, we can print
     * out a B+ tree as a DOT file which we can then convert into a nice
     * picture of the B+ tree. tree.toDot() returns the contents of DOT file
     * which illustrates the B+ tree. The details of the file itself is not at
     * all important, just know that if you call tree.toDot() and save the
     * output to a file called tree.dot, then you can run this command
     *
     *   dot -T pdf tree.dot -o tree.pdf
     *
     * to create a PDF of the tree.
     */
    public String toDot() {
        // (proj4_integration): Update the following line
        // the BplusTree#toDot needs to read the tree, so we grant it the LockType.S lock.
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.S);

        List<String> strings = new ArrayList<>();
        strings.add("digraph g {" );
        strings.add("  node [shape=record, height=0.1];");
        strings.add(root.toDot());
        strings.add("}");
        return String.join("\n", strings);
    }

    /**
     * This function is very similar to toDot() except that we write
     * the dot representation of the B+ tree to a dot file and then
     * convert that to a PDF that will be stored in the src directory. Pass in a
     * string with the ".pdf" extension included at the end (ex "tree.pdf").
     */
    public void toDotPDFFile(String filename) {
        String tree_string = toDot();

        // Writing to intermediate dot file
        try {
            java.io.File file = new java.io.File("tree.dot");
            FileWriter fileWriter = new FileWriter(file);
            fileWriter.write(tree_string);
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Running command to convert dot file to PDF
        try {
            Runtime.getRuntime().exec("dot -T pdf tree.dot -o " + filename).waitFor();
        } catch (IOException e) {
            e.printStackTrace();
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new BPlusTreeException(e.getMessage());
        }
    }

    public BPlusTreeMetadata getMetadata() {
        return this.metadata;
    }

    /**
     * Returns the largest number d such that the serialization of a LeafNode
     * with 2d entries and an InnerNode with 2d keys will fit on a single page.
     */
    public static int maxOrder(short pageSize, Type keySchema) {
        int leafOrder = LeafNode.maxOrder(pageSize, keySchema);
        int innerOrder = InnerNode.maxOrder(pageSize, keySchema);
        return Math.min(leafOrder, innerOrder);
    }

    /** Returns the partition number that the B+ tree resides on. */
    public int getPartNum() {
        return metadata.getPartNum();
    }

    /**
     * Save the new root page number and update the tree's metadata.
     **/
    private void updateRoot(BPlusNode newRoot) {
        this.root = newRoot;

        metadata.setRootPageNum(this.root.getPage().getPageNum());
        metadata.incrementHeight();
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction != null) {
            transaction.updateIndexMetadata(metadata);
        }
    }

    private void typecheck(DataBox key) {
        Type t = metadata.getKeySchema();
        if (!key.type().equals(t)) {
            String msg = String.format("DataBox %s is not of type %s", key, t);
            throw new IllegalArgumentException(msg);
        }
    }

    // Iterator ////////////////////////////////////////////////////////////////
    private class BPlusTreeIterator implements Iterator<RecordId> {
        // (proj2): Add whatever fields and constructors you want here.
        private Iterator<RecordId> idIterator;
        private LeafNode currLeaf;

        public BPlusTreeIterator() {
            currLeaf = root.getLeftmostLeaf();
            idIterator = root.getLeftmostLeaf().scanAll();
        }

        public BPlusTreeIterator(DataBox key) {
            currLeaf = root.get(key);
            idIterator = currLeaf.scanGreaterEqual(key);
        }


        @Override
        public boolean hasNext() {
            // (proj2): implement

            if (idIterator == null) {
                return false;
            }

            if (idIterator.hasNext()) {
                return true;
            }

            Optional<LeafNode> sibling = currLeaf.getRightSibling();
            while (sibling.isPresent()) {
                // 有可能存在前面的LeafNode为空，但右边的LeafNode还有值的情况
                // 所以需要使用循环来遍历有的LeafNode
                /**
                 *
                 *                               inner
                 *                               +----+----+----+----+
                 *                               | 10 | 20 |    |    |
                 *                               +----+----+----+----+
                 *                              /     |     \
                 *                         ____/      |      \____
                 *                        /           |           \
                 *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
                 *   |    |    |    |    |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
                 *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
                 *   leaf0                  leaf1                  leaf2
                 */
                currLeaf = sibling.get();
                idIterator = currLeaf.scanAll();
                if (idIterator.hasNext()) {
                    return true;
                } else {
                    // 遍历其rightSibling
                    sibling = currLeaf.getRightSibling();
                }
            }
            return false;
        }

        @Override
        public RecordId next() {
            // (proj2): implement
            if (idIterator.hasNext()) {
                return idIterator.next();
            }
            return null;
        }
    }
}
