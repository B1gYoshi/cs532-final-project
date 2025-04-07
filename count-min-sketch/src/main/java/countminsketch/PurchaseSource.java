package countminsketch;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

// DetailedTransactionSource class implemented from the DetailedSource class
// Changed functionality from statically generating transactions to instead randomly generating DetailedTransactions
public class PurchaseSource implements SourceFunction<Purchase> {
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Purchase> sourceContext) throws Exception {
        long timestamp = 0L;

        // manually generated transactions to verify fraud detection logic
        Purchase[] transactions = {new Purchase("1", "product1", "Computer"), new Purchase("2", "product2", "Home&Kitchen")};
        int transactionIndex = 0;

        while (running){
            // part of manually generated transactions to verify fraud detection logic
            sourceContext.collect(transactions[transactionIndex++ % transactions.length]);

            Thread.sleep(100);
        }
    }


    // function to terminate the generation of the DetailedTransactions
    @Override
    public void cancel() {
        running = false;
    }
}
