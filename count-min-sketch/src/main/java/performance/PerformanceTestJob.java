package performance;

import countminsketch.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import stream.Purchase;
import stream.PurchaseSource;

import java.time.Duration;

/**
 * job for testing purchase analysis pipeline performance.
 */
public class PerformanceTestJob {
    public static void main(String[] args) throws Exception {
        // settings for the test, command line args
        
        // String testName = args.length > 0 ? args[0] : "latency_test";// for later, when we have multiple tests
        // int reportInterval = args.length > 1 ? Integer.parseInt(args[1]) : 1000; //for printing
        // int testSeconds = args.length > 2 ? Integer.parseInt(args[2]) : 60; // total runtime of test

        String testName = "test_1";
        int reportInterval = 1000;
        int testSeconds = 60;    
        
        // test parameters
        int CORES = 10;
        int M = 100;
        int K = 5;
        
        System.out.println("Starting test: " + testName);
        System.out.println("report interval: " + reportInterval);
        System.out.println("duration: " + testSeconds + " seconds");
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(CORES);
        
        // pipeline with monitors
        DataStream<Purchase> purchases = env
                .addSource(new PurchaseSource())
                .name("transactions")
                .map(new PerformanceMonitor<>("source", reportInterval))
                .name("source-monitor");
        
        DataStream<CMSResult> cmsOutputs = purchases
                .map(new RandomKeySelector(CORES))
                .keyBy((KeySelector<Tuple2<Integer, Purchase>, Integer>) value -> value.f0)
                .window(SlidingProcessingTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .process(new CountMinSketch(M, K))
                .map(new PerformanceMonitor<>("cms", reportInterval))
                .name("cms-processor");
        
        DataStream<CMSMergedResult> cmsMerged = cmsOutputs
                .keyBy(CMSResult::getWindowTimestamp)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
                .process(new CMSMerge(M))
                .map(new PerformanceMonitor<>("merge", reportInterval))
                .name("merged-cms");
                
        DataStream<PurchaseAlert> alerts = cmsMerged
                .process(new CMSAnalysis(M))
                .map(new PerformanceMonitor<>("alerts", reportInterval))
                .name("cms-results");
        
        // reporting sink
        alerts.addSink(new PerformanceReportingSink<>(
                testName,
                "performance_tes_" + testName + ".csv",
                reportInterval))
                .name("performance-sink");
        


        // timeout
        if (testSeconds > 0) {
            final Thread mainThread = Thread.currentThread();
            new Thread(() -> {
                Thread.sleep(testSeconds * 1000);
                System.out.println("\nTime limit reached");
                mainThread.interrupt();

            }).start();
        }
        

        env.execute("Performance Test - " + testName);

    }
}
