package com.amazon.cluster;

/**
 * Callbacks to applications upon shared state changes within a cluster.
 * 
 * Note that simultaneous callbacks in multiple threads may occur.  Hence,
 * implementers must be prepared to deal with synchronization issues.
 */
public interface ClusterListener
{
  /**
   * Status events occur when a new cluster member becomes known to us, or
   * when a known member becomes unreachable, or when a known member that
   * was unreachable becomes reachable again. An unreachable cluster member
   * may be down, or just on the other side of a network partition; we do
   * not distinguish these cases. 
   * 
   * When this listener is first registered via Cluster.addStateListener(),
   * noteMemberStatus() is called for each known member independent of its
   * reachability.  One could then query ClusterMember.isReachable() if one
   * needs reachability information at that time.
   * 
   * @param member the member in the cluster whose status has changed
   */
  public void noteMemberStatus(ClusterMember member);

  /**
   * State change events occur when the local view of state changes, whether
   * by Cluster.setLocalState() or by propagation from remote members.  Call
   * ClusterMember.getValue(name) to obtain the current state.
   * 
   * @param member the cluster member whose state has changed
   * @param name the key to the name-value pair that has changed
   */
  public void noteStateChange(ClusterMember member, String name);
  
  /**
   * All listeners are notified of shutdown via this callback.
   */
  public void noteShutdown();
}

