package com.amazon.messaging.seqstore.v3.internalInterface;

/**
 * A unique identifier for the store. The storeName is the name as used on disk
 * and should be unique to the manager. The GroupName is an identifier used
 * externally to attribute some relationship between different stores. It is not
 * used within SeqStore itself. The Id is an identifier for uniqueness within a
 * group. It is not required to be globally unique.
 * 
 * @author kaitchuc
 */
public interface StoreId {


    /**
     * The full store name. This includes both the group and the id
	 */
    String getStoreName();

    /**
     * The group for the store.
     */
    String getGroupName();

    /**
     * The id for the store. This should be unique for the group
     */
    String getId();

}
