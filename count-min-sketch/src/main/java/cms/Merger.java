package cms;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Merger extends ProcessWindowFunction<Sketch, List<HotKey>, Long, TimeWindow> {

    @Override
    public void process(Long key, Context context, Iterable<Sketch> sketches, Collector<List<HotKey>> collector) {
        // Combine hot keys of each sketch
        Map<String, HotKey> merged = new HashMap<>();
        for (Sketch sketch : sketches) {
            for (HotKey hotKey : sketch.getHotKeys()) {
                merged.merge(hotKey.getKey(), hotKey, (a, b) -> {
                    a.setEstimate(a.getEstimate() + b.getEstimate());
                    return a;
                });
            }
        }
        collector.collect(new ArrayList<>(merged.values()));
    }
}
