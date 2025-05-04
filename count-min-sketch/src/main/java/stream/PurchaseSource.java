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
<<<<<<< HEAD

            // Rate-limit
            Thread.sleep(1L);

            // Busy wait for under 1 ms delay
            /*
            long start = System.nanoTime();
            while (start + 100_000 >= System.nanoTime());
            */
=======
            // Thread.sleep(10L);//rate limiting
            count++;
>>>>>>> yan
        }

        running = false;
    }

    @Override
    public void cancel() {
        running = false;
    }
}
