package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // (proj4_part2): implement
        long txNum = transaction.getTransNum();
        LockType oldLockType = getExplicitLockType(transaction);

        if (this.readonly) {
            throw new UnsupportedOperationException("This context is readonly!");
        }
        // check DuplicateLockRequest
        if (lockman.getLockType(transaction, name) != LockType.NL) {
            // the solution of xuzishuo is `lockman.getLockType(transaction, name) == lockType`
            // but I think it should be LockType.NL;
            throw new DuplicateLockRequestException("A lock is already held by the transaction.");
        }

        // check whether the request is invalid
        if (!isValidAcquireRequest(transaction, lockType)) {
            LockType parentLock = parentContext().getExplicitLockType(transaction);
            throw new InvalidLockException("Can not acquire " + lockType + " with " + parentLock + " as its parents");
        }

        // acquire the lock
        this.lockman.acquire(transaction, name, lockType);

        // update numChildLocks
        if (parentContext() != null) {
            parentContext().numChildLocks.putIfAbsent(txNum, 0);
            parentContext().numChildLocks.put(txNum, parentContext().getNumChildren(transaction) + 1);
        }
    }

    /**
     * Helper method to check whether the release of `transaction`'s lock on `name` is valid.
     * @param transaction the given transaction
     * @return {@code true} if the release is valid.
     *         {@code false} otherwise.
     */
    private boolean isValidRelease(TransactionContext transaction) {
        // To check whether a release is valid, we consider two cases:
        // case 1: if the child(ren) do(es) not hold any locks, return true
        // case 2: otherwise, return false.
        long txNum = transaction.getTransNum();
        if (numChildLocks.getOrDefault(txNum, 0) > 0) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // (proj4_part2): implement
        LockType oldLockType = lockman.getLockType(transaction, name);
        long txNum = transaction.getTransNum();
        if (readonly) {
            throw new UnsupportedOperationException("The context is readonly.");
        }

        if (oldLockType == LockType.NL) {
            throw new NoLockHeldException("There is no lock on " + name + " held by " + transaction.getTransNum());
        }

        if (!isValidRelease(transaction)) {
            throw new InvalidLockException("The lock cannot be released due to the violation of multigranularity locking constraints");
        }

        // release `transaction`'s lock on `name`
        this.lockman.release(transaction, name);

        if (parentContext() != null) {
            if (parentContext().getNumChildren(transaction) < 1) {
                throw new DatabaseException("Undefined behavior.");
            }
            parentContext().numChildLocks.put(txNum, parentContext().getNumChildren(transaction) - 1);
        }
    }

    /**
     * Helper method that checks whether the requested promote is valid
     * @param transaction the given transaction
     *        newLockType the requested new locktype
     * @returns {@code true} if the promotion is valid
     *          {@code false} otherwise.
     */
    private boolean isValidPromote(TransactionContext transaction, LockType newLockType) {
        // if newLockType is substitutable for oldLockType, return true
        LockType oldLockType = getExplicitLockType(transaction);

        if (hasSIXAncestor(transaction) && newLockType == LockType.SIX) {
            // disallow promotion of SIX lock if an ancestor has SIX, since it would be redundant
            return false;
        }

        if (LockType.substitutable(newLockType, oldLockType)) {
            return oldLockType != newLockType;
        }

        // Or the newLockType is SIX and oldLockType is IS/IX/S
        if (newLockType.equals(LockType.SIX) &&
                (oldLockType.equals(LockType.S) || oldLockType.equals(LockType.IS) || oldLockType.equals(LockType.IX))) {
            return true;
        }
        // otherwise, the `promote` is invalid, return false;
        return false;
    }
    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // (proj4_part2): implement
        long txNum = transaction.getTransNum();
        LockType oldLockType = getExplicitLockType(transaction);

        if (readonly) {
            throw new UnsupportedOperationException("Can not promote on a readonly context!");
        }

        if (oldLockType == LockType.NL) {
            throw new NoLockHeldException("Transaction " + txNum + " does not hold any lock on " + name);
        }

        if (oldLockType == newLockType) {
            throw new DuplicateLockRequestException("Transaction "+ txNum + " already has a " + newLockType + " lock on " + name);
        }

        if (!isValidPromote(transaction, newLockType)) {
            throw new InvalidLockException("Can not promote " + oldLockType + " to " + newLockType);
        }

        lockman.promote(transaction, name, newLockType);

        // release all the S/IS locks on descendants and update the numChildLocks
        if ((oldLockType == LockType.IS || oldLockType == LockType.IX) && newLockType == LockType.SIX) {
            LockContext parentContext = parentContext();
            List<ResourceName> sisDescendants = sisDescendants(transaction);
            for (ResourceName sisDescendant : sisDescendants) {
                // release all the S/IS descendants
                lockman.release(transaction, sisDescendant);
                if (parentContext != null) {
                    parentContext.numChildLocks.put(txNum, getNumChildren(transaction) - 1);
                }
            }
        }

    }

    /**
     * Helper method that returns the descendants of this context under this level.
     * @param   transaction the given transaction
     *
     * @return  A list that contains the ResourceName of all the descendants of the given transaction
     */
    private List<ResourceName> getDescendants(TransactionContext transaction) {
        List<ResourceName> descendants = new ArrayList<>();
        for (Lock lock : lockman.getLocks(transaction)) {
            if (lock.name.isDescendantOf(this.name)) {
                descendants.add(lock.name);
            }
        }
        return descendants;
    }
    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        LockType currLockType = getExplicitLockType(transaction);
        long transNum = transaction.getTransNum();
        if (readonly) {
            throw new UnsupportedOperationException("Can not escalate on a readonly context!");
        }

        if (currLockType == LockType.NL) {
            throw new NoLockHeldException("The current transaction " + transNum + " has no lock at this level");
        }


        LockType escalateLock = LockType.S;
//        List<ResourceName> descendants = getDescendants(transaction);
        List<ResourceName> toReleaseDesc = new ArrayList<>();
        for (Lock lock : lockman.getLocks(transaction)) {
            if (lock.name.equals(this.name)) {
                if (lock.lockType.equals(LockType.IX) || lock.lockType.equals(LockType.SIX)) {
                    escalateLock = LockType.X;
                } else if (lock.lockType.equals(LockType.S) || lock.lockType.equals(LockType.X)) {
                    // prevent repetitive escalate
                    return;
                }
            } else if (lock.name.isDescendantOf(this.name)) {
                LockType childLockType = lock.lockType;
                if (childLockType.equals(LockType.IX) || childLockType.equals(LockType.X) || childLockType.equals(LockType.SIX)) {
                    escalateLock = LockType.X;
                }
                toReleaseDesc.add(lock.name);
            }
        }

        if (escalateLock != currLockType && LockType.substitutable(escalateLock, currLockType)) {
            lockman.promote(transaction, this.name, escalateLock); // promote the current lock
            for (ResourceName toRelease : toReleaseDesc) {
                // release all the descendant locks
                lockman.release(transaction, toRelease);
            }
        }
        // in escalate, we need to update the numChildLocks of this level, not the parent level.
        numChildLocks.put(transNum, 0);
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // (proj4_part2): implement
        // getExplicitLockType only cares about the type of the lock that `transaction` holds at *this* level.
        // We just directly use the LockManager::getLockType to find out the result.
        return this.lockman.getLockType(transaction, name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // (proj4_part2): implement
        LockType explicitLock = getExplicitLockType(transaction);
        if (explicitLock != LockType.NL) {
            // the transaction has explicit lock on this level, return it.
            return explicitLock;
        }

        LockContext currContext = parentContext();
        while (currContext != null && explicitLock == LockType.NL) {
            explicitLock = currContext.getExplicitLockType(transaction);
            currContext = currContext.parentContext();
        }

        if (explicitLock == LockType.IS || explicitLock == LockType.IX) {
            return LockType.NL;
        } else if (explicitLock == LockType.SIX) {
            return LockType.S;
        } else {
            return explicitLock;
        }
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // (proj4_part2): implement
        LockContext currParent = parentContext();
        while (currParent != null) {
            if (currParent.getExplicitLockType(transaction) == LockType.SIX) {
                return true;
            }
            currParent = currParent.parentContext();
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // (proj4_part2): implement
        List<ResourceName> sisDescendants = new ArrayList<>();
        for (Lock lock: lockman.getLocks(transaction)) {
            // scan the locks of `transaction` to get all the S/IS descendants
            if (lock.name.isDescendantOf(this.name) &&
                    (lock.lockType == LockType.S || lock.lockType == LockType.IS)) {
                sisDescendants.add(lock.name);
            }
        }
        return sisDescendants;
    }

    /**
     * Helper method to check whether the acquire lockType is valid
     * @param   transaction the given transaction
     *          lockType the requested locktype
     * @return  {@code true} if the request is valid
     *          otherwise, return {@code false}
     */
    private boolean isValidAcquireRequest(TransactionContext transaction, LockType lockType) {
        if (parentContext() == null) {
            return true;
        }

        if (!LockType.canBeParentLock(parentContext().getExplicitLockType(transaction), lockType)) {
            return false;
        } else {
            return true;
        }
    }



    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

