package cms;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import stream.Purchase;

public class WindowCMS extends ProcessWindowFunction<Purchase, Sketch, Integer, TimeWindow> {

    @Override
    public void process(Integer key, Context context, Iterable<Purchase> elements, Collector<Sketch> collector) {
        // Build sketch of purchases
        Sketch sketch = new Sketch(20, 20, 5);
        for (Purchase purchase : elements) {
            sketch.update(purchase.getCategory());
        }
        collector.collect(sketch); // Emit the final sketch
    }
}