package performance;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * monitor for pipeline throughput measurement.
 */
public class PerformanceMonitor<T> extends RichMapFunction<T, T> {
    
    private String point;
    private int interval;
    
    private long count;
    private long startTime;
    private long lastReportTime;
    
    public PerformanceMonitor(String point, int interval) {
        this.point = point;
        this.interval = interval;
    }
    

    public void open(Configuration parameters) throws Exception {
        startTime = System.currentTimeMillis();
        lastReportTime = startTime;
        count = 0;
    }
    
    @Override
    public T map(T value) throws Exception {
        count++;
        
        if (count % interval == 0) {
            long now = System.currentTimeMillis();
            long timeElapsed = now - lastReportTime;
            long totalTime = now - startTime;
            
            double recent = (1000.0 * interval) / timeElapsed;
            double overall = (1000.0 * count) / totalTime;
            
            // point lists the stage in the pipeline
            System.out.printf("[%s] Count: %d | Recent: %.1f/s | Overall: %.1f/s | Time: %.1fs%n",
                    point, count, recent, overall, totalTime / 1000.0);
            
            lastReportTime = now;
        }
        
        return value;
    }
    
    @Override
    public void close() throws Exception {
        long totalTime = System.currentTimeMillis() - startTime;
        double avg = count > 0 ? (1000.0 * count) / totalTime : 0;
        
        System.out.printf("[%s] FINAL: %d events in %.1f seconds (%.1f/s)%n", 
                point, count, totalTime / 1000.0, avg);
    }
}
