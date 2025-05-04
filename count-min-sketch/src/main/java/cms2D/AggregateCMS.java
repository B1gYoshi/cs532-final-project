package cms2D;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import stream.Purchase;
import java.util.TreeSet;

public class AggregateCMS implements AggregateFunction<Tuple2<Integer, Purchase>, WindowResult, WindowResult> {
    private final int width;
    private final int depth;
    private final int maxHotKeys;

    public AggregateCMS(int width, int depth, int maxHotKeys) {
        this.width = width;
        this.depth = depth;
        this.maxHotKeys = maxHotKeys;
    }

    @Override
    public WindowResult createAccumulator() {
        Sketch sketch = new Sketch(width, depth);
        TreeSet<HotKey> hotKeys = new TreeSet<>();
        return new WindowResult(sketch, hotKeys);
    }

    @Override
    public WindowResult add(Tuple2<Integer, Purchase> tuple, WindowResult result) {
        String category = tuple.f1.getCategory();
        int estimate = result.getSketch().update(category);

        // Update potential hot key
        TreeSet<HotKey> hotKeys = result.getHotKeys();
        HotKey hotKey = new HotKey(category, estimate);
        hotKeys.remove(hotKey);
        hotKeys.add(hotKey);
        if (hotKeys.size() > maxHotKeys) {
            hotKeys.pollFirst();
        }
        return result;
    }

    @Override
    public WindowResult getResult(WindowResult result) {
        return result;
    }

    @Override
    public WindowResult merge(WindowResult a, WindowResult b) {
        // Unused under our tumbling window scheme
        return null;
    }
}