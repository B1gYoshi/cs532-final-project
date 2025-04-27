package stream;

import org.apache.flink.annotation.Public;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;

@Public
public class PurchaseSource implements SourceFunction<Purchase> {
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Purchase> context) throws Exception {
        // Pull purchases from generator with delay
        PurchaseGenerator generator = new PurchaseGenerator();
        while (running) {
            synchronized (context.getCheckpointLock()) {
                context.collect(generator.next());
            }
            Thread.sleep(100L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
