package cms2D;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import stream.Purchase;
import java.util.TreeSet;

public class WindowCMS extends ProcessWindowFunction<Tuple2<Integer, Purchase>, WindowResult, Integer, TimeWindow> {
    private final int width;
    private final int depth;
    private final int maxHotKeys;

    public WindowCMS(int width, int depth, int maxHotKeys) {
        this.width = width;
        this.depth = depth;
        this.maxHotKeys = maxHotKeys;
    }

    @Override
    public void process(Integer key, Context context, Iterable<Tuple2<Integer, Purchase>> tuples, Collector<WindowResult> collector) {
        // Build sketch and track hot keys
        TreeSet<HotKey> hotKeys = new TreeSet<>();
        Sketch sketch = new Sketch(width, depth);

        // Process stream
        for (Tuple2<Integer, Purchase> tuple : tuples) {
            String category = tuple.f1.getCategory();
            int min = sketch.update(category);

            // Update potential hot key
            HotKey hotKey = new HotKey(category, min);
            hotKeys.remove(hotKey);
            hotKeys.add(hotKey);
            if (hotKeys.size() > maxHotKeys) {
                hotKeys.pollFirst();
            }
        }

        // Emit sketch, hot keys, and window stamp
        collector.collect(new WindowResult(sketch, hotKeys, context.window().getStart()));
    }
}