package cms2D;

import metrics.PurchaseMetricsCollector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import stream.Purchase;
import stream.PurchaseSource;
import java.time.Duration;
import java.util.List;

public class PurchaseAnalysisJob {
    public static void main(String[] args) throws Exception {
        final int NUM_CORES = 10;       // Level of parallelism
        final int WIDTH = 10;           // Length of the rows in each sketch
        final int DEPTH = 5;            // Number of rows in each sketch
        final int MAX_HOT_KEYS = 2;     // Size limit for local top categories set

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(NUM_CORES);

        // Create the stream
        DataStream<Purchase> purchases = env
            .addSource(new PurchaseSource())
            .name("purchases");

        // Record metrics during run
        /*
        DataStream<Purchase> purchases = env
            .addSource(new PurchaseSource())
            .name("transactions")
            .map(new PurchaseMetricsCollector())
            .name("metrics")
            .disableChaining();
         */

        // Run CMS algorithm on each worker over windows
        DataStream<WindowResult> sketches = purchases
            .map(new RandomKeySelector(NUM_CORES))
            .keyBy(value -> value.f0)
            .window(SlidingProcessingTimeWindows.of(
                Duration.ofSeconds(10),
                Duration.ofSeconds(5)
            ))
            .process(new WindowCMS(WIDTH, DEPTH, MAX_HOT_KEYS))
            .name("cms2D");

        // Merge sketches and local popularity lists
        DataStream<List<HotKey>> topCategories = sketches
            .keyBy(WindowResult::getStamp)
            .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
            .process(new Merger(WIDTH, DEPTH))
            .name("merger");

        topCategories
            .print()
            .name("print")
            .setParallelism(1);

        env.execute("count min sketch");
    }
}