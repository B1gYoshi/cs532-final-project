package stream;

import org.apache.flink.annotation.Public;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;

@Public
public class PurchaseSource implements SourceFunction<Purchase> {
    private volatile boolean running = true;
    private static final long MAX_EVENTS = 100_000_000L; // Limit to 10 million

    @Override
    public void run(SourceContext<Purchase> context) throws Exception {
        PurchaseGenerator generator = new PurchaseGenerator();

        long count = 0;
        while (running && count < MAX_EVENTS) {
            synchronized (context.getCheckpointLock()) {
                context.collect(generator.next());
            }
            count++; //max out at 10 million rather than sleep
            // Thread.sleep(100L);
        }

        running = false;
    }

    @Override
    public void cancel() {
        running = false;
    }
}
