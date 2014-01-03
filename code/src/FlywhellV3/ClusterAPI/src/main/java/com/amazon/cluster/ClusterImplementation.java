package com.amazon.cluster;

import java.net.URI;

/**
 * The factory class that all implementations must provide.  Applications
 * instantiate an implementation and then call newMember() or newObserver()
 * as needed.  
 * 
 * Implementations of this class must be thread safe with minimal thread
 * contention so to enable a singleton to be used within a JVM.
 */
public abstract class ClusterImplementation
{
    /**
     * Create a new instance observing the named cluster.  This instance will
     * not be a member of the cluster, only a client 'spying' on the cluster.
     *
     * To initiate observing, one must first setSeeds() where running cluster
     * members may be found, and then joinCluster().  If additional seeds are
     * discovered after joining the cluster, they may be added at any time.
     * 
     * @param clusterName the name of the cluster agreed to by all members
     * @return a new cluster observer for the named cluster
     */
    public abstract ClusterObserver newObserver(String clusterName);

    /**
     * Create a new member instance of the named cluster.  This instance will
     * not bind its local end point and become a reachable member of the cluster 
     * until joinCluster() is called.  Before calling joinCluster(), one must
     * first setSeeds() where running cluster members may be found. Additional
     * seeds may be added at any time as they are discovered.
     *
     * The given clusterName must not be empty.  Note that a local membershipURI
     * will be assigned by the implemetation.  This can be retrieved by calling
     * getMembershipURI() on the LocalMember.
     *
     * Note that this is equivalent to calling newMember(clusterName, null).
     * 
     * @param clusterName the name of the cluster agreed to by all members
     * @return a new cluster member for the named cluster with a membership URI
     */
    public abstract LocalMember newMember(String clusterName);

    /**
     * Create a new member instance of the named cluster.  This instance will
     * not bind its local end point and become a reachable member of the cluster 
     * until joinCluster() is called.  Before calling joinCluster(), one must
     * first setSeeds() where running cluster members may be found. Additional
     * seeds may be added at any time as they are discovered.
     *
     * The given clusterName must not be empty, and the local membershipURI must 
     * truely be locally resolvable and bindable as the implementation requires.
     *
     * Note that the scheme of the local membershipURI must match implementation
     * requirements. (For instance, the SWIM implementation requires 'swim:tcp'.)
     * 
     * @param clusterName the name of the cluster agreed to by all members
     * @param localMembershipURI the membership URI, or null to have one assigned
     * @return a new cluster member for the named cluster with a membership URI
     */
    public abstract LocalMember newMember(String clusterName, URI membershipURI);
}
