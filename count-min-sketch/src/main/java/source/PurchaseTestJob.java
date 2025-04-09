package source;

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
}