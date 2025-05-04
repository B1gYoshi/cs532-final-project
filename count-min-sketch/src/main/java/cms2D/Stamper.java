package cms2D;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Stamper extends ProcessWindowFunction<WindowResult, WindowResult, Integer, TimeWindow> {
    @Override
    public void process(Integer key, Context context, Iterable<WindowResult> results, Collector<WindowResult> collector) {
        for (WindowResult result : results) {
            result.setStamp(context.window().getStart());
            collector.collect(result);
        }
    }
}