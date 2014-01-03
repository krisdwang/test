package com.amazon.messaging.seqstore.v3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;

public class StoreIdImplTest {

    @Test
    public void testBasicStoreId() {
        StoreId id = new StoreIdImpl("a", "b");
        assertEquals("a", id.getGroupName());
        assertEquals("b", id.getId());
        
        // Test that the store id can be reconstructed from the store name
        StoreId newId = new StoreIdImpl(id.getStoreName());
        assertEquals(id, newId);
        assertEquals(id.getStoreName(), newId.getStoreName());
        assertEquals(id.getGroupName(), newId.getGroupName());
        assertEquals(id.getId(), newId.getId());
    }
    
    @Test
    public void testStoreIdEscaping() {
        final String group = "a:b::c:d::";
        final String id = "e::f:g";

        StoreId storeId = new StoreIdImpl(group, id);
        assertEquals(group, storeId.getGroupName());
        assertEquals(id, storeId.getId());

        String escapedGroup = StoreIdImpl.escapeGroup(group);
        assertTrue( storeId.getStoreName().startsWith( escapedGroup ) );
        assertEquals( group, StoreIdImpl.unescapeGroup( escapedGroup ) );
        
        // Test that the store id can be reconstructed from the store name
        StoreId newId = new StoreIdImpl(storeId.getStoreName());
        assertEquals(storeId, newId);
        assertEquals(storeId.getStoreName(), newId.getStoreName());
        assertEquals(storeId.getGroupName(), newId.getGroupName());
        assertEquals(storeId.getId(), newId.getId());
    }

    @Test
    public void testStoreIdOnlyGroup() {
        StoreId id = new StoreIdImpl("a:b::c:d:", null);
        assertEquals("a:b::c:d:", id.getGroupName());
        assertEquals(null, id.getId());
        
        // Test that the store id can be reconstructed from the store name
        StoreId newId = new StoreIdImpl(id.getStoreName());
        assertEquals(id, newId);
        assertEquals(id.getStoreName(), newId.getStoreName());
        assertEquals(id.getGroupName(), newId.getGroupName());
        assertEquals(id.getId(), newId.getId());
    }
}
