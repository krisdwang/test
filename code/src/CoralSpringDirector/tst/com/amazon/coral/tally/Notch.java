package com.amazon.coral.tally;

public class Notch {
    private final Object[] details;

    public Notch(Object... details) {
        this.details = details;
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof Notch)) {
            return false;
        }

        Notch cmp = (Notch) object;

        if (details.length != cmp.details.length) {
            return false;
        }

        for (int i = 0; i < details.length; i++) {
            Object o1 = details[i];
            Object o2 = cmp.details[i];

            if ((o1 == null && o2 != null) || (o1 != null && o2 == null)) {
                return false;
            }
            if (!o1.equals(o2)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        for (Object o : details) {
            hash += o.hashCode();
        }
        return hash;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("NOTCH [");
        boolean first = true;
        for (Object o : details) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(o.toString());
        }
        sb.append("] @");
        sb.append(String.format("%08X", hashCode()));

        return sb.toString();
    }
}
