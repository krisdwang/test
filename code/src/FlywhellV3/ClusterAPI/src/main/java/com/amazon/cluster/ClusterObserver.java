package com.amazon.cluster;

import java.net.URI;
import java.util.List;
import java.util.Collection;
import java.util.concurrent.Executor;

/**
 * A Cluster is a set of one or more member instances sharing state and 
 * reachability information across a network. The local instance, which is 
 * obtained from a ClusterImplementation, represents the view of the cluster 
 * from this local perspective.
 * 
 * The state model is simple: the local member owns a set of name-value pairs
 * that it may update at any time via setLocalState().  No other member may
 * update another's state, and so there is no conflict resolution.  Updated
 * state will eventually propagate to all reachable members.  However, no
 * guarantees about timeliness are provided.
 * 
 * One may also send an asynchronous message to another member via send().
 * These messages are completely independent of the state model.
 * 
 * Implementations of this class must be thread safe with minimal thread
 * contention so to enable a singleton to be within a JVM.
 */
public interface ClusterObserver 
{
    /**
     * Return the name of the cluster we are observing.
     *
     * @return name of the cluster we are observing
     */
    public String getClusterName();

    /**
     * Return a collection of known cluster members, whether reachable or not,
     * and including the local member (if any). Note that this collection is
     * a snapshot at the time of this call, an hence, may be out of date by
     * the time results are returned.
     *
     * The returned list is owned by the caller, and hence may be modified by
     * the caller without issue.  Implementations must not keep a reference to
     * the returned list so to enable garbage collection.
     *
     * @return snapshot of cluster members, whether reachable or not
     */
    public List<ClusterMember> getMembershipSnapshot();

    /**
     * Explicitly specify the membership expiration timeout for this local view.
     * If any member becomes unreachable for a period longer than this timeout, 
     * the isMembershipExpired() flag will be set to true and a callback to 
     * noteMemberStatus() will be scheduled.  After that callback the expired 
     * member instance and all its associated state is purged from memory.
     *
     * The default for this timeout is Integer.MAX_VALUE seconds, which simply 
     * means that the isMembershipExpired() flag will never be set and members 
     * will never be purged from the local membership set.
     *
     * @param timeoutInSeconds duration of unreachability after which a member
     * is consider expired
     */
    public void setMembershipExpirationTimeout(int timeoutInSeconds);
    
    /**
     * Add a cluster listener to the chain of listeners for this local view. 
     * When the state of the cluster changes, a callback to this listener is
     * made asynchronously using a single threaded executor dedicated to this
     * listener.  This is the same as addListener(listener, null);
     * 
     * Adding a listener that is equal to a currently registered listener is
     * a no-op.
     * 
     * @param listener to be added to this cluster listener chain with a
     * defaulted single threaded executor
     */
    public void addListener(ClusterListener listener);
    
    /**
     * Add a cluster listener to the chain of listeners for this local view. 
     * When the state of the cluster changes, a callback to this listener is
     * made asynchronously using the given executor. Hence, if a multithreaded
     * executor is provided, the listener must be properly thread safe.
     * 
     * Adding a listener that is equal to a currently registered listener is
     * a no-op.
     * 
     * @param listener to be added to this cluster listener chain
     * @param executor to be used to call back to this listener, or null
     * to default to a single threaded executor
     */
    public void addListener(ClusterListener listener, Executor executor);
    
    /**
     * Removes the cluster listener and associated executor from the listener 
     * chain.  If no such listener is registered, this is a no-op.
     * 
     * @param listener to be removed from this cluster listener chain
     */
    public void removeListener(ClusterListener listener);

    /**
     * As member end points become known, call this method so to inform the
     * underlying implementation of new or removed possibilities.
     *
     * @param seedURIs to be used to find other members within the cluster
     */
    public void setSeeds(Collection<URI> seedURIs);
  
    /**  
     * Establishes the local end point and joins the cluster. Subsequent calls
     * to this may be used to specify additional seeds and enlarge the scope
     * of the cluster.  One may call setLocalState() prior to calling this in 
     * order to join with an initial state.
     *
     * @param seeds a collection of endpoints where the members may be found
     */
    public void joinCluster() throws JoinFailureException;

    /**
     * Shut down our local view and release all resources associated with our
     * participation in this cluster. All registered listeners are notified via
     * a callback to noteShutdown. 
     *
     * Once shutdown completes, any attempt to modify state will result in a 
     * java.lang.IllegalStateException.
     * 
     * WARNING: a shutdown cannot be reversed, so make this call with caution.
     */
    public void shutdownAndReleaseResources();
}
