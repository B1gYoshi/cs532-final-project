package countminsketch;

import org.apache.flink.api.common.functions.RichMapFunction;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.api.java.tuple.Tuple2;

// import stream.Purchase;
// import stream.PurchaseSource;
import stream.Purchase;
import stream.PurchaseSource;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import metrics.MetricsCollector;
import metrics.PurchaseMetricsCollector;



/**
 * Skeleton code for the datastream walkthrough
 */

// DetailedFraudDetectionJob class based off of original FraudDetectionJob from the Flink walkthrough
// Modified class to use all new classes: DetailedTransaction, DetailedAlert, DetailedFraudDetector, and DetailedAlertSink

public class PurchaseAnalysisJob {
    public static void main(String[] args) throws Exception {
        final int NUM_CORES = 10;
        final int M = 100;
        final int K = 5;

        // Set up local execution environment with parallelism
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(NUM_CORES);

        DataStream<Purchase> purchases = env
                .addSource(new PurchaseSource())
                .name("transactions")
                .map(new PurchaseMetricsCollector())
                .name("metrics")
                .disableChaining();   

        DataStream<CMSResult> cmsOutputs = purchases
                .map(new RandomKeySelector(NUM_CORES))
                .keyBy((KeySelector<Tuple2<Integer, Purchase>, Integer>) value -> value.f0)
                .window(SlidingProcessingTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .process(new CountMinSketch(M, K))
                .name("fraud-detector");

        DataStream<CMSMergedResult> cmsMerged = cmsOutputs
                .keyBy(CMSResult::getWindowTimestamp)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
                .process(new CMSMerge(M))
                .name("merged-cms");

        DataStream<PurchaseAlert> alerts = cmsMerged
                .process(new CMSAnalysis(M))
                .name("cms-results");

        alerts
                .addSink(new SinkFunction<PurchaseAlert>() {
                    @Override
                    public void invoke(PurchaseAlert value, SinkFunction.Context context) {
                        System.out.println(value.toString());
                    }
                })
                .name("send-alerts");

        env.execute("Detailed Fraud Detection");
    }
}

class RandomKeySelector extends RichMapFunction<Purchase, Tuple2<Integer, Purchase>> {

    private final int NUM_CORES;
    private Random rand;

    public RandomKeySelector (int NUM_CORES) {
        this.NUM_CORES = NUM_CORES;
        rand = new Random();
    }


    @Override
    public Tuple2<Integer, Purchase> map(Purchase purchase) throws Exception {
        return Tuple2.of(rand.nextInt(NUM_CORES), purchase);
    }
}