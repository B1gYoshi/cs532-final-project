package cms;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import stream.Purchase;

public class WindowCMS extends ProcessWindowFunction<Tuple2<Integer, Purchase>, Sketch, Integer, TimeWindow> {
    private final int width;
    private final int depth;
    private final int maxHotKeys;

    public WindowCMS(int width, int depth, int maxHotKeys) {
        this.width = width;
        this.depth = depth;
        this.maxHotKeys = maxHotKeys;
    }

    @Override
    public void process(Integer key, Context context, Iterable<Tuple2<Integer, Purchase>> elements, Collector<Sketch> collector) {
        // Build sketch of purchases
        Sketch sketch = new Sketch(width, depth, maxHotKeys);
        for (Tuple2<Integer, Purchase> purchase : elements) {
            sketch.update(purchase.f1.getCategory());
        }

        // Stamp and emit sketch
        sketch.setStamp(context.window().getStart());
        collector.collect(sketch);
    }
}