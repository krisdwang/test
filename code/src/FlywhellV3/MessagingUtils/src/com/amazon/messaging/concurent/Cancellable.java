package com.amazon.messaging.concurent;



/**
 * A minimal interface, which allows certain asynchronous activity to be cancelled. It is preferable to use this instead
 * of implementing {@link Future} just for the {@link Future#cancel(boolean)} call.
 */
public interface Cancellable {

    /**
     * For convenience only, this returns an implementation of {@link Cancellable}, which has no effect. Useful for the
     * cases, where operation is already cancelled.
     */
    public static final Cancellable noopCancellable = new Cancellable () {
        @Override
        public boolean cancel () {
            return false;
        }
    };

    /**
     * Requests that an outstanding asynchronous activity be cancelled and returns immediately. This method should not
     * block. Only the first invocation of this method should have effect, with all consecutive invocations ignoring it
     * if cancel has already been requested.
     */
    public boolean cancel ();
}
