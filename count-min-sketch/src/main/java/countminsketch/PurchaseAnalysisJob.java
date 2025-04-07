package countminsketch;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;


/**
 * Skeleton code for the datastream walkthrough
 */

// DetailedFraudDetectionJob class based off of original FraudDetectionJob from the Flink walkthrough
// Modified class to use all new classes: DetailedTransaction, DetailedAlert, DetailedFraudDetector, and DetailedAlertSink

public class PurchaseAnalysisJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Purchase> purchases = env
                .addSource(new PurchaseSource())
                .name("transactions");

        DataStream<PurchaseAlert> alerts = purchases
                .keyBy(Purchase::getCategory)
                .process(new KeyedProcessFunction<String, Purchase, PurchaseAlert>() {
                    @Override
                    public void processElement(Purchase transaction, Context context, Collector<PurchaseAlert> collector) {
                        PurchaseAlert alert = new PurchaseAlert();
                        alert.setMessage(transaction.toString());

                        collector.collect(alert);
                    }
                })
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
