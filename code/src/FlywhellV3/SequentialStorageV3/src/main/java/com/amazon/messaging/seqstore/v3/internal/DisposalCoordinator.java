package com.amazon.messaging.seqstore.v3.internal;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * This class implements mutual exclusion of a thread disposing a resource from
 * all threads invoking other methods on it.
 * 
 * @author poojag
 */
@SuppressWarnings(value="UL_UNRELEASED_LOCK", justification="This class is a lock")
public final class DisposalCoordinator {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public void waitToExecuteNonDisposalCall() {
        lock.readLock().lock();
    }

    public void wakeupThreadsAfterNonDisposalCall() {
        lock.readLock().unlock();
    }

    public void waitToDispose() {
        lock.writeLock().lock();
    }

    public void wakupThreadsAfterDisposal() {
        lock.writeLock().unlock();
    }
}
