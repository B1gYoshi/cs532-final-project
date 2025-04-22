package cms;

public class HotKey implements Comparable<HotKey> {
    private final String key;
    private final int estimate;

    public HotKey(String key, int estimate) {
        this.key = key;
        this.estimate = estimate;
    }

    public String getKey() {
        return key;
    }

    public int getEstimate() {
        return estimate;
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
}
