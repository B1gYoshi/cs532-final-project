package cms;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.*;

public class Merger extends ProcessWindowFunction<Sketch, List<HotKey>, Long, TimeWindow> {

    @Override
    public void process(Long key, Context context, Iterable<Sketch> sketches, Collector<List<HotKey>> collector) {
        // Combine hot keys of each sketch
        Map<String, HotKey> map = new HashMap<>();
        for (Sketch sketch : sketches) {
            for (HotKey hotKey : sketch.getHotKeys()) {
                map.merge(hotKey.getKey(), hotKey, (a, b) -> {
                    a.setEstimate(a.getEstimate() + b.getEstimate());
                    return a;
                });
            }
        }

        // Sort and emit combined keys
        List<HotKey> merged = new ArrayList<>(map.values());
        merged.sort(Collections.reverseOrder());
        collector.collect(merged);
    }
}
