package cms2D;

import java.util.TreeSet;

public class WindowResult {
    private final Sketch sketch;
    private final TreeSet<HotKey> hotKeys;
    private final long stamp;

    public WindowResult(Sketch sketch, TreeSet<HotKey> hotKeys, long stamp) {
        this.sketch = sketch;
        this.hotKeys = hotKeys;
        this.stamp = stamp;
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
}
