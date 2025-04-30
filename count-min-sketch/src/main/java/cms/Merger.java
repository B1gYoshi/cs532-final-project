package cms;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.*;

public class Merger extends ProcessWindowFunction<WindowResult, List<HotKey>, Long, TimeWindow> {
    private final int width;
    private final int depth;

    public Merger(int width, int depth) {
        this.width = width;
        this.depth = depth;
    }

    @Override
    public void process(Long key, Context context, Iterable<WindowResult> sketches, Collector<List<HotKey>> collector) {
        Map<String, HotKey> map = new HashMap<>();
        Sketch merged = new Sketch(width, depth);

        // Merge hot keys and sketches
        for (WindowResult result : sketches) {
            merged.merge(result.getSketch());
            for (HotKey hotKey : result.getHotKeys()) {
                map.put(hotKey.getKey(), hotKey);
            }
        }

        // Update hot keys based on merged sketch
        for (HotKey hotKey : map.values()) {
            hotKey.setEstimate(merged.estimate(hotKey.getKey()));
        }

        // Sort and emit final keys
        List<HotKey> result = new ArrayList<>(map.values());
        result.sort(Collections.reverseOrder());
        collector.collect(result);
    }
}
