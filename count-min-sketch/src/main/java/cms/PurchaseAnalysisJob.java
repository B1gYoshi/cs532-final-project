package cms;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
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
        final int NUM_CORES = 10;
        final int WIDTH = 10;
        final int DEPTH = 10;
        final int MAX_HOT_KEYS = 2;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(NUM_CORES);

        DataStream<Purchase> purchases = env
            .addSource(new PurchaseSource())
            .name("purchases");

        DataStream<Sketch> sketches = purchases
            .map(new RandomKeySelector(NUM_CORES))
            .keyBy((KeySelector<Tuple2<Integer, Purchase>, Integer>) value -> value.f0)
            .window(SlidingProcessingTimeWindows.of(
                Duration.ofSeconds(10),
                Duration.ofSeconds(5)
            ))
            .process(new WindowCMS(WIDTH, DEPTH, MAX_HOT_KEYS))
            .name("cms");

        DataStream<List<HotKey>> topCategories = sketches
            .keyBy(Sketch::getStamp)
            .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
            .process(new Merger())
            .name("merger");

        topCategories
            .print()
            .name("print")
            .setParallelism(1);

        env.execute("count min sketch");
    }
}