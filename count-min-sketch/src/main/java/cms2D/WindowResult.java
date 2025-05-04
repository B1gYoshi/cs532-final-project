package cms2D;

import java.util.Set;

public class WindowResult {
    private final Sketch sketch;
    private final Set<HotKey> hotKeys;
    private final long stamp;

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
