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

            // Rate-limit
            Thread.sleep(1L);

            // Busy wait for under 1 ms delay
            /*
            long start = System.nanoTime();
            while (start + 100_000 >= System.nanoTime());
            */
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
