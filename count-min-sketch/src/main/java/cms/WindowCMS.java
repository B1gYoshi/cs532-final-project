package cms;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import stream.Purchase;

public class WindowCMS extends ProcessAllWindowFunction<Purchase, Sketch, TimeWindow> {
    private final int width;
    private final int depth;
    private final int maxHotKeys;

    public WindowCMS(int width, int depth, int maxHotKeys) {
        this.width = width;
        this.depth = depth;
        this.maxHotKeys = maxHotKeys;
    }

    @Override
    public void process(Context context, Iterable<Purchase> elements, Collector<Sketch> collector) {
        // Build sketch of purchases
        Sketch sketch = new Sketch(width, depth, maxHotKeys);
        for (Purchase purchase : elements) {
            sketch.update(purchase.getCategory());
        }

        // Stamp and emit sketch
        sketch.setStamp(context.window().getStart());
        collector.collect(sketch);
    }
}