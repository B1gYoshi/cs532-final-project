package stream;

import org.apache.flink.annotation.Public;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;

import java.io.IOException;

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

            // Use Thread.sleep(0, ...) for under 1 ms
            Thread.sleep(1L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
