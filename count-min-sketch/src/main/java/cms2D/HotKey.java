package cms2D;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class HotKey implements Comparable<HotKey> {
    private String key;
    private int estimate;

    public HotKey(String key, int estimate) {
        this.key = key;
        this.estimate = estimate;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getEstimate() {
        return estimate;
    }

    public void setEstimate(int estimate) {
        this.estimate = estimate;
    }

    @Override
    public int compareTo(HotKey other) {
        int keyComp = this.key.compareTo(other.key);
        if (keyComp == 0) {
            return 0;
        }

        // Compare estimate, break ties by key
        int estimateComp = Integer.compare(this.estimate, other.estimate);
        return estimateComp == 0 ? keyComp : estimateComp;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
            .append("key", key)
            .append("estimate", estimate)
            .toString();
    }
}
