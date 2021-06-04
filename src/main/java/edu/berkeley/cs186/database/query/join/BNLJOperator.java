package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Block Nested Loop Join algorithm.
 */
public class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction),
                leftColumnName, rightColumnName, transaction, JoinType.BNLJ
        );
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().estimateStats().getNumPages();
        int numRightPages = getRightSource().estimateIOCost();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               getLeftSource().estimateIOCost();
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join.
     * Look over the implementation in SNLJOperator if you want to get a feel
     * for the fetchNextRecord() logic.
     */
    private class BNLJIterator implements Iterator<Record>{
        // Iterator over all the records of the left source
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right source
        private BacktrackingIterator<Record> rightSourceIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftBlockIterator;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightPageIterator;
        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        private BNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            this.fetchNextLeftBlock();

            this.rightSourceIterator = getRightSource().backtrackingIterator();
            this.rightSourceIterator.markNext();
            this.fetchNextRightPage();

            this.nextRecord = null;
        }

        /**
         * Fetch the next block of records from the left source.
         * leftBlockIterator should be set to a backtracking iterator over up to
         * B-2 pages of records from the left source, and leftRecord should be
         * set to the first record in this block.
         *
         * If there are no more records in the left source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         */
        private void fetchNextLeftBlock() {
            // (proj3_part1): implement
            if (this.leftSourceIterator.hasNext()) {
                // 注意这里是getLeftSource().getSchema();
                this.leftBlockIterator = QueryOperator.getBlockIterator(this.leftSourceIterator, BNLJOperator.this.getLeftSource().getSchema(),
                        BNLJOperator.this.numBuffers-2); // maxPage=B-2, fetch up to B-2 pages
                if (this.leftBlockIterator.hasNext()) {
                    leftBlockIterator.markNext();
                    this.leftRecord = this.leftBlockIterator.next();
                }
            }

        }

        /**
         * Fetch the next page of records from the right source.
         * rightPageIterator should be set to a backtracking iterator over up to
         * one page of records from the right source.
         *
         * If there are no more records in the right source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         */
        private void fetchNextRightPage() {
            // (proj3_part1): implement
            if (this.rightSourceIterator.hasNext()) {
                // 注意这里是getRightSource().getSchema();
                this.rightPageIterator = QueryOperator.getBlockIterator(this.rightSourceIterator, BNLJOperator.this.getRightSource().getSchema(),
                                                                        1); // maxPage set to 1, cause only need to fetch 1 page;
                if (rightPageIterator.hasNext()) {
                    rightPageIterator.markNext(); // 为什么都要markNext()呢？
                }
            }
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         *
         * You may find JoinOperator#compare useful here. (You can call compare
         * function directly from this file, since BNLJOperator is a subclass
         * of JoinOperator).
         */
        private Record fetchNextRecord() {
            // (proj3_part1): implement
            if (leftRecord == null) {
                // The left source was empty, nothing to fetch
                return null;
            }

            while (true) {
                if (this.rightPageIterator.hasNext()) {
                    // case 1: the right page iterator has a value to yield,
                    // join it if there's a match
                    Record rightRecord = this.rightPageIterator.next();
                    if (compare(this.leftRecord, rightRecord) == 0) {
                        return this.leftRecord.concat(rightRecord);
                    }
                } else if (this.leftBlockIterator.hasNext()) {
                    // case 2: the right page iterator doesn't have a value to yield,
                    // but the left block iterator does;
                    // Advance left and reset right;
                    this.leftRecord = this.leftBlockIterator.next();
                    this.rightPageIterator.reset();
                    this.rightPageIterator.markNext();
                } else if (this.rightSourceIterator.hasNext()) {
                    // case 3: neither right page nor left block iterator have values to yield
                    // but there are more right pages.
                    // fetch new right pages and reset left
                    fetchNextRightPage();
                    leftBlockIterator.reset();
                    leftBlockIterator.markNext();
                    leftRecord = leftBlockIterator.next();
                } else if (this.leftSourceIterator.hasNext()) {
                    // case 4: neither right page nor left block iterators have values to yield
                    // nor are there're more right pages (rightSourceIterator.hasNext() == false)
                    // but there are still left blocks (leftSourceIterator.hasNext() == true)
                    // fetch new left block, reset right
                    fetchNextLeftBlock();
                    this.rightSourceIterator.reset();
                    rightSourceIterator.markNext();
                    fetchNextRightPage();
                } else {
                    // if you are here then there are no more records to fetch
                    return null;
                }
            }
        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }
}
