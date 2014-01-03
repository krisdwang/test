package com.amazon.cluster;

/**
 * A Cluster is a set of one or more member instances sharing state and 
 * reachability information across a network. The local member, which is 
 * obtained from ClusterImplementation.newMember(), represents the view of 
 * the cluster from this local perspective.
 * 
 * The state model is simple: the local member owns a set of name-value pairs
 * that it may update at any time via setLocalState().  No other member may
 * update another's state, and so there is no conflict resolution.  Updated
 * state will eventually propagate to all reachable members.  However, no
 * guarantees about timeliness are provided.
 * 
 * Implementations of this class must be thread safe with minimal thread
 * contention so to enable a singleton to be within a JVM.
 */
public interface LocalMember extends ClusterMember, ClusterObserver
{
  /**
   * Set the cluster state value associated with the local member and
   * the given name. A null value effectively removes this state entry.
   * 
   * Changes to state will eventually propagate out to all reachable
   * members, but no guarantees as to timeliness are provided.  It is
   * possible to change a value several times locally before it makes it's
   * way to other members.  It is guaranteed, however, that all reachable
   * members will eventually become consistent. 
   * 
   * When a state change is detected, whether local or remote, all listeners 
   * will be called as described in ClusterListener.noteStateChange().
   *  
   * @param name key of the value to be set or updated
   * @param value a byte array that represents the new state value, or null 
   * to effectively remove this state entry
   */
  public void setLocalState(String name, byte[] value);

  /**
   * Set the cluster state value associated with the local member and
   * the given name from a buffer starting at the offset for length bytes. 
   * If buffer is null with offset and length both zero, the value for the
   * given name is effectively removed this state entry.
   * 
   * Changes to state will eventually propagate out to all reachable
   * members, but no guarantees as to timeliness are provided.  It is
   * possible to change a value several times locally before it makes it's
   * way to other members.  It is guaranteed, however, that all reachable
   * members will eventually become consistent. 
   * 
   * When a state change is detected, whether local or remote, all listeners 
   * will be called as described in ClusterListener.noteStateChange().
   *  
   * @param name key of the value to be set or updated
   * @param buffer a byte array that contains the new state value, or null 
   * to effectively remove this state entry
   * @param offset index into the given buffer where the value starts
   * @param length number of bytes assigned to the resultant value
   * @throws IndexOutOfBoundsException if offset and length do not properly
   * fit within the bounds of the buffer, or either is non-zero when the 
   * buffer is null
   */
  public void setLocalState(String name, byte[] buffer, int offset, int length);
}
