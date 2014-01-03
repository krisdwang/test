package com.amazon.cluster;

import java.util.Date;
import java.util.Map;
import java.net.URI;

/**
 * A cluster is composed of members, some reachable and some not currently
 * reachable.  One member is known as the local member, the member whose
 * view is being represented.
 * 
 * The state model for a member is a simple name-value map.  Only the local
 * member can update this state, and only via LocalMember.setLocalState().
 * Hence, state values may change dynamically under the covers, which will
 * result in callbacks to register listeners.  To get a stable snapshot of
 * the current state, call getStateSnapshot().
 */
public interface ClusterMember
{
  /**
   * Return the cluster of which this is a member independent whether this
   * member is currently reachable.
   * 
   * @return name of the cluster of which this is a member
   */
  public String getClusterName();

  /**
   * Return the membership URI associated with this member independent of
   * its reachability.
   *
   * @return URI of this member
   */
  public URI getMembershipURI();
  
  /**
   * Return true if this member is the local member of the cluster.  Only
   * one member of a cluster is local, and the view of the cluster is in
   * relation to that member.
   *
   * If isLocalMember() is true, this member == the local member, and may
   * be cast to type LocalMember.
   * 
   * @return true if this member is the local member
   */
  public boolean isLocalMember();
  
  /**
   * Return true if this member is currently reachable.  An unreachable 
   * member may be temporarily or permanently down, or just on the other 
   * side of a network partition; we do not distinguish these cases. Note, 
   * however, the race condition between the result of this call and the 
   * true state of the remote member.  
   * 
   * @return true if this member is currently considered reachable
   */
  public boolean isReachable();
  
  /**
   * Return the timestamp when this member was last reachable.  If this
   * member was never reachable during the life time of this cluster, a
   * null will be returned.  If it is currently reachable, the timestamp
   * of the last known communication with the member will be returned.
   *  
   * @return timestamp of the last reachable time, or null if never reachable
   */
  public Date getWhenLastReachable();

  /**
   * Returns true if this member has been unreachable for a period greater
   * than the membership expiration timeout.  Generally, this flag is set
   * immediately prior to scheduling callbacks to noteMemberStatus(), after
   * which this member and all associated state will be wiped from memory.
   *
   * @return true if this member's membership has expired
   */
  public boolean isMembershipExpired();

  /**
   * Return the state value for the given name.  If no state exists for that
   * name, a null is returned.  Note the race condition between this call
   * and background updates.
   * 
   * @param name the key of the name-value pair owned by this member
   * @return value of the name-value pair, or null if this value is not set
   */
  public byte[] getState(String name);
  
  /**
   * Return a snapshot of the entire state model associated with this member.
   * Note the race condition between this call and background updates.
   * 
   * @return snapshot of the state model for this member
   */
  public Map<String,byte[]> getStateSnapshot();
}
