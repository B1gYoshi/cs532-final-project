package cms1D;

import java.util.Set;

public class CMSResult {
    private final int[] cmsArray;
    private final long windowTimestamp;
    private final Set<String> topKCategories;

    public CMSResult (int[] cmsArray, long timestamp, Set<String> topKCategories) {
        this.cmsArray = cmsArray;
        this.windowTimestamp = timestamp;
        this.topKCategories = topKCategories;
    }

    public long getWindowTimestamp() {
        return windowTimestamp;
    }

    public int[] getCmsArray() {
        return cmsArray;
    }

    public Set<String> getTopKCategories() {
        return topKCategories;
    }
}
