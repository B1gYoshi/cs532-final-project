package cms2D;

import java.util.Set;

public class WindowResult {
    private Sketch sketch;
    private Set<HotKey> hotKeys;
    private long stamp;

    public WindowResult(Sketch sketch, Set<HotKey> hotKeys, long stamp) {
        this.sketch = sketch;
        this.hotKeys = hotKeys;
        this.stamp = stamp;
    }

    public Sketch getSketch() {
        return sketch;
    }

    public Set<HotKey> getHotKeys() {
        return hotKeys;
    }

    public long getStamp() {
        return stamp;
    }
}
