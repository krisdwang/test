/**
 * 
 */
package com.amazon.messaging.seqstore.v3.config;

public enum DistributionPolicy {
    /**
     * Clusters will only exist in one local area
     */
    LocalAreaOnly(0),
    /**
     * Will try, but not required to span wide area
     */
    WideAreaDesired(1),
    /**
     * Enqueues must span 2 areas to be successful
     */
    Span2AreasRequired(2),
    /**
     * Enqueues must span 3 areas to be successful
     */
    Span3AreasRequired(3);

    /**
     * Used by {@link DistributionPolicy#fromIntValue(int)}
     */
    private final static DistributionPolicy[] policies_ = new DistributionPolicy[] { LocalAreaOnly,
            WideAreaDesired, Span2AreasRequired, Span3AreasRequired };

    private final int intValue_;

    private DistributionPolicy(int intValue) {
        intValue_ = intValue;
    }

    public int intValue() {
        return intValue_;
    }

    public static DistributionPolicy fromIntValue(int value) {
        if ((value < 0) || (value > 3)) {
            throw new IllegalArgumentException("Unknown distribution policy with int value=" + value);
        }
        return policies_[value];
    }
    
    public int getTargetAZs() {
        switch( this ) {
        case LocalAreaOnly:
            return 1;
        case WideAreaDesired:
        case Span2AreasRequired:
            return 2;
        case Span3AreasRequired:
            return 3;
        default:
            throw new IllegalArgumentException( "Unknown distribution policy " + this );
        }
    }
    
    public int getMinAZs() {
        switch( this ) {
        case LocalAreaOnly:
        case WideAreaDesired:
            return 1;
        case Span2AreasRequired:
            return 2;
        case Span3AreasRequired:
            return 3;
        default:
            throw new IllegalArgumentException( "Unknown distribution policy " + this );
        }
    }
}
