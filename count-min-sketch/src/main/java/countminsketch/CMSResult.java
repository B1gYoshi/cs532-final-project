package countminsketch;

public class CMSResult {
    private final int[] cmsArray;
    private final long windowTimestamp;

    public CMSResult (int[] cmsArray, long timestamp) {
        this.cmsArray = cmsArray;
        this.windowTimestamp = timestamp;
    }

    public long getWindowTimestamp() {
        return windowTimestamp;
    }

    public int[] getCmsArray() {
        return cmsArray;
    }
}
