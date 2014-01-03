package com.amazon.dse.discovery;

/**
 * A registry service-independent record
 */

public class RegistryEntryRecord {
    private String name_;

    private int port_ = 0;

    // 10 min refresh from master to slave directory server
    private int ttl_ = 600;

    // does the user like to override directory service info
    private boolean commitFlag_ = false;

    public boolean equals(Object obj) {
	boolean result = false;
	if (obj == this)
	    return true;
	if ((null != obj) && (obj instanceof RegistryEntryRecord)) {
	    RegistryEntryRecord rec = (RegistryEntryRecord) obj;
	    if (name_.equalsIgnoreCase(rec.getName()) && (port_ == rec.getPort()))
		result = true;
	}

	return result;
    }

    public int hashCode() {
	return (toString().hashCode());
    }

    public String toString() {
	return name_ + ":" + port_;
    }
    
    public String getName() {
	return name_;
    }

    /**
     * set the name of the cluster
     */
    public void setName(String name) {
	if (name == null) {
	    throw new IllegalArgumentException("Name cannot be null");
	}

	this.name_ = name;
    }

    public int getPort() {
	return port_;
    }

    public void setPort(int port) {
	if (port < 0) {
	    throw new IllegalArgumentException("Port cannot be negative");
	}
	this.port_ = port;
    }

    public int getTTL() {
	return ttl_;
    }

    /**
     * Set the TTL associated with the record - only defaults used with DNS
     * system
     */
    public void setTTL(int ttl) {
	if (ttl <= 0) {
	    throw new IllegalArgumentException("TTL cannot be negative OR 0");
	}
	this.ttl_ = ttl;
    }

    /**
     * A client can use this flag to chose to enable a policy - if this is set,
     * the local info overrides the directory service info if there is a
     * discrepancy
     */
    public void setCommitFlag(boolean value) {
	commitFlag_ = value;
    }

    public boolean getCommitFlag() {
	return commitFlag_;
    }
}
