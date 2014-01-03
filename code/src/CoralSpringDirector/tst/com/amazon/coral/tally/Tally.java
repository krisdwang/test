package com.amazon.coral.tally;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Tally {
    public class IndexedObject {
        public Object object;
        public int index;

        public IndexedObject(Object object, int index) {
            this.object = object;
            this.index = index;
        }

        @Override
        public boolean equals(Object o) {
            return object.equals(o);
        }

        @Override
        public String toString() {
            return String.format("[%4d] %s", index, object.toString());
        }
    }

    private final static Log log = LogFactory.getLog(Tally.class);

    LinkedList<IndexedObject> allEvents = new LinkedList<IndexedObject>();
    Map<Integer, LinkedList<IndexedObject>> sequenceMap = new HashMap<Integer, LinkedList<IndexedObject>>();
    int currentIndex = 0;

    public synchronized void add(Object event) {
        int hash = event.hashCode();

        IndexedObject indexedObject = new IndexedObject(event, currentIndex++);

        LinkedList<IndexedObject> events = null;

        if (!sequenceMap.containsKey(hash)) {
            events = new LinkedList<IndexedObject>();
            sequenceMap.put(hash, events);
        } else {
            events = sequenceMap.get(hash);
        }

        events.add(indexedObject);
        allEvents.add(indexedObject);
    }

    public synchronized boolean happened(Object event) {
        return indexOf(event) > 0;
    }

    public synchronized int indexOf(Object event, int after) {
        int hash = event.hashCode();

        if (!sequenceMap.containsKey(hash)) {
            return -1;
        }

        LinkedList<IndexedObject> objects = sequenceMap.get(hash);

        // DO NOT use objects.indexOf(event) because it checks with event.equals(indexedObject)
        for (IndexedObject indexObject : objects) {
            if (indexObject.equals(event) && (indexObject.index > after)) {
                return indexObject.index;
            }
        }

        return -1;
    }

    public synchronized int indexOf(Object event) {
        int hash = event.hashCode();

        if (!sequenceMap.containsKey(hash)) {
            return -1;
        }

        LinkedList<IndexedObject> objects = sequenceMap.get(hash);

        // DO NOT use objects.indexOf(event) because it checks with event.equals(indexedObject)
        for (IndexedObject indexObject : objects) {
            if (indexObject.equals(event)) {
                return indexObject.index;
            }
        }

        return -1;
    }

    public synchronized boolean inSequence(Object... events) {
        if (events.length < 2) {
            throw new IllegalArgumentException("At least 2 events needed");
        }

        Object e1 = events[0];
        int i1 = this.indexOf(e1);
        if (i1 < 0) {
            throw new IllegalArgumentException(String.format("Did not happen: %s", e1));
        }

        for (int i = 1; i < events.length; i++) {
            Object e2 = events[i];

            int i2 = this.indexOf(e2, i1);

            if (i2 < 0) {
                throw new IllegalArgumentException(String.format("Did not happen: %s", e2));
            }

            if (i2 <= i1) {
                log.error(String.format("Violation at index: %d (actual order %d, %d)", i, i1, i2));
                log.error(String.format("Objects: \n#%d %s\n#%s %s\n", i1, e1, i2, e2));
                return false;
            }

            i1 = i2;
            e1 = e2;
        }

        return true;
    }

    public boolean isEmpty() {
        return sequenceMap.isEmpty();
    }

    public Collection<IndexedObject> getIndexedEvents() {
        return allEvents;
    }
}
