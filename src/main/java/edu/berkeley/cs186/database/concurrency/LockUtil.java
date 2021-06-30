package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {


    /**
     * Helper method that ensures you have the appropriate locks on all ancestors
     */
    private static void ensureSufficientAncestorLockHeld(LockContext lockContext, TransactionContext transaction, LockType sufficientType) {
        LockContext parentContext = lockContext.parentContext();
        Deque<LockContext> ancestors = new ArrayDeque<>();

        while (parentContext != null) {
            // we need to get to the top-level first and then start acquiring/promoting locks
            // so we use a stack to store the lock acquire/promote list.
            ancestors.push(parentContext);
            parentContext = parentContext.parentContext();
        }

        while (!ancestors.isEmpty()) {
            LockContext currContext = ancestors.pop();
            LockType currLock = currContext.getExplicitLockType(transaction);
            if (currLock == sufficientType) {
                // already has the sufficientType, skip this level
                continue;
            } else if (currLock == LockType.NL) {
                // lock on this context is NL, we need to acquire the sufficient lock
                currContext.acquire(transaction, sufficientType);
            } else if (LockType.substitutable(sufficientType, currLock)) {
                // sufficient lock can substitute the original lock on parentContext, we promote the original lock
                currContext.promote(transaction, sufficientType);
            } else {
                if ((sufficientType.equals(LockType.IX) && currLock.equals(LockType.S)) ||
                        (sufficientType.equals(LockType.S) && currLock.equals(LockType.IX))) {
                    currContext.promote(transaction, LockType.SIX);
                }
            }
        }
    }

    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null | lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // (proj4_part2): implement
        // phase 1: ensure all the ancestors hold a sufficient lock
        if (requestType == LockType.S) {
            // ensure all the ancestors at least hold a IS lock
            ensureSufficientAncestorLockHeld(lockContext, transaction, LockType.IS);
        } else if (requestType == LockType.X) {
            // ensure all the ancestors at lease hold a IX lock
            ensureSufficientAncestorLockHeld(lockContext, transaction, LockType.IX);
        }
        // phase 2: acquire the lock
        if (LockType.substitutable(effectiveLockType, requestType)) {
            return;
        } else if (explicitLockType == LockType.IX && requestType == LockType.S) {
            // promote lock of the current lockContext to SIX
            lockContext.promote(transaction, LockType.SIX);
        } else if (explicitLockType.isIntent()) {
            // the current lock is intent, we escalate it (grant it a non-intent lock) and
            // then call ensureSufficientLockHeld again.
            lockContext.escalate(transaction);
            ensureSufficientLockHeld(lockContext, requestType);
        } else {
            // if get here, the explicitLock must be NL or S or X, we need to acquire the lock if it's NL
            // and promote if the explicitLock is S/X
            if (explicitLockType == LockType.NL) {
                lockContext.acquire(transaction, requestType);
            } else {
                try {
                    lockContext.promote(transaction, requestType);
                } catch (InvalidLockException e) {
                    lockContext.escalate(transaction);
                    ensureSufficientLockHeld(lockContext, requestType);
                }
            }

        }
    }

    // add any helper methods you want


}

