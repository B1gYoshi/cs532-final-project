package countminsketch;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;

import java.time.Duration;
import java.util.Random;

/**
 * Skeleton code for the datastream walkthrough
 */

// DetailedFraudDetectionJob class based off of original FraudDetectionJob from the Flink walkthrough
// Modified class to use all new classes: DetailedTransaction, DetailedAlert, DetailedFraudDetector, and DetailedAlertSink

public class PurchaseAnalysisJob {
    public static void main(String[] args) throws Exception {
        final int NUM_CORES = 10;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(NUM_CORES);

        DataStream<Purchase> purchases = env
                .addSource(new PurchaseSource())
                .name("transactions");

        DataStream<PurchaseAlert> alerts = purchases
                .map(new RoundRobinKeySelector())
                .keyBy( new KeySelector<Tuple2<Integer, Purchase>, Integer>() {
                    @Override
                    public Integer getKey(Tuple2<Integer, Purchase> value) {
                        return value.f0;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new CountMinSketch())
                .name("fraud-detector");

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

class RoundRobinKeySelector extends RichMapFunction<Purchase, Tuple2<Integer, Purchase>> {

    @Override
    public Tuple2<Integer, Purchase> map(Purchase purchase) throws Exception {
        return Tuple2.of(getRuntimeContext().getIndexOfThisSubtask(), purchase);
    }
}
