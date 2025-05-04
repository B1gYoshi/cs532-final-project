package cms2D;

import java.util.TreeSet;

public class WindowResult {
    private final Sketch sketch;
    private final TreeSet<HotKey> hotKeys;
    private long stamp;
    private long total;

    public WindowResult(Sketch sketch, TreeSet<HotKey> hotKeys) {
        this.sketch = sketch;
        this.hotKeys = hotKeys;
    }

    public Sketch getSketch() {
        return sketch;
    }

    public TreeSet<HotKey> getHotKeys() {
        return hotKeys;
    }

    public long getStamp() {
        return stamp;
    }

    public void setStamp(long stamp) {
        this.stamp = stamp;
    }
}
