package stream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PurchaseTestJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Print stream of purchases
        env
            .addSource(new PurchaseSource())
            .name("purchases")
            .keyBy(Purchase::getProductId)
            .print()
            .name("print-purchases");

        env.execute("Purchase Test Job");
    }
    @Override
    public void open(Configuration parameters) {
        // Initialize metrics when the source operator starts
        this.startTime = System.currentTimeMillis();
        

        getRuntimeContext().getMetricGroup()
            .gauge("sourceStartTime", new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return startTime;
                }
            });
            
        // Add a gauge that shows how long the source has been running (in seconds)
        getRuntimeContext().getMetricGroup()
            .gauge("upTimeSeconds", new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return (System.currentTimeMillis() - startTime) / 1000;
                }
            });
    }
}