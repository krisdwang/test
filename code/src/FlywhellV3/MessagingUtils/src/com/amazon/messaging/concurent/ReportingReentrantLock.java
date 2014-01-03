package com.amazon.messaging.concurent;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import com.amazon.messaging.utils.collections.FactoryMap;
import com.amazon.messaging.utils.collections.ValueFactories;

/**
 * An extension of ReentrantLock that adds the ability to report what threads
 * are waiting for the lock and where the thread that has the lock is. Note
 * that the reporting is best effort and does not represent a perfect snapshot
 * of the lock's state.
 * 
 * @author stevenso
 *
 */
public class ReportingReentrantLock extends ReentrantLock {
    private static final long serialVersionUID = 1L;

    private static class StackTraceWrapper {
        private final StackTraceElement[] stackTrace;
        
        private StackTraceWrapper(StackTraceElement[] stackTrace) {
            super();
            this.stackTrace = stackTrace;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(stackTrace);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            StackTraceWrapper other = (StackTraceWrapper) obj;
            if (!Arrays.equals(stackTrace, other.stackTrace))
                return false;
            return true;
        }
        
        public StackTraceElement[] getStackTrace() {
            return stackTrace;
        }
    }
    
    public ReportingReentrantLock() {
        super();
    }
    
    public ReportingReentrantLock(boolean fair) {
        super(fair);
    }

    /**
     * Returns a string representing the lock state and the threads
     * waiting for it or holding it.
     * @return
     */
    public String reportLockState() {
        StringBuilder report = new StringBuilder();
        
        Thread owningThread = getOwner();
        Collection<Thread> waitingThreads = getQueuedThreads();
        
        if( owningThread != null ) {
            report.append( "Owned by: " + owningThread.getName() + "\n" );
            StackTraceElement[] stackTrace = owningThread.getStackTrace();
            appendStackTrace(report, stackTrace, "\t");
        } else {
            report.append( "Unlocked\n" );
        }
        
        if( !waitingThreads.isEmpty() ) {
            report.append( "Waiting threads:\n" );
            Map<StackTraceWrapper, List<String>> stackTraceMap = 
                    new FactoryMap<StackTraceWrapper, List<String>>( ValueFactories.<String>getListFactory() );
            
            for( Thread thread : waitingThreads ) {
                StackTraceElement[] stackTrace = thread.getStackTrace();
                stackTraceMap.get( new StackTraceWrapper( stackTrace ) ).add( thread.getName() );
            }
            
            for( Map.Entry<StackTraceWrapper, List<String>> entry : stackTraceMap.entrySet() ) {
                report.append( "\tThreads: " ).append( entry.getValue() ).append( "\n" );
                appendStackTrace( report, entry.getKey().getStackTrace(), "\t" );
                report.append("\n");
            }
        } else {
            report.append( "No waiting threads.\n" );
        }
        
        return report.toString();
    }

    private void appendStackTrace(StringBuilder report, StackTraceElement[] stackTrace, String prefix) {
        if( stackTrace != null ) {
            for (StackTraceElement element : stackTrace ) {
                report.append(prefix).append("at " ).append( element ).append("\n");
            }
        }
    }
}
