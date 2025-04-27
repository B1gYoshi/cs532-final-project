package performance;

import org.apache.flink.configuration.Configuration;
//problematic:
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * sink for performance metrics.
 */
public class PerformanceReportingSink<T> implements SinkFunction<T> {
    
    private String testName;
    private String outputFile;
    private int reportInterval;
    
    private long count;
    private long startTime;
    private long lastReportTime;
    
    private List<Double> rates;
    private PrintWriter writer;
    
    public PerformanceReportingSink(String testName, String outputFile, int reportInterval) {
        this.testName = testName;
        this.outputFile = outputFile;
        this.reportInterval = reportInterval;
    }
    

    // called when the sink is initialized in the Flink job
    // sets up the output file and initializes the counters 
    public void open(Configuration parameters) throws Exception {
        count = 0;
        startTime = System.currentTimeMillis();
        lastReportTime = startTime;
        rates = new ArrayList<>();
        
        // outputs to file
        if (outputFile != null && !outputFile.isEmpty()) {
            writer = new PrintWriter(new FileWriter(outputFile));
            writer.println("time,count,rate,seconds");
        }
    }
    
    @Override
    // called for each element in stream. tracks the number of elements processed and time
    // it just counts the number of elements processed and reports the rate
    public void invoke(T value, Context context) throws Exception {
        count++;
        
        if (count % reportInterval == 0) {
            long now = System.currentTimeMillis();
            long interval = now - lastReportTime;
            long total = now - startTime;
            
            double rate = (1000.0 * reportInterval) / interval;// rate for the last interval
            double overall = (1000.0 * count) / total;
            
            rates.add(rate);
            
            System.out.printf("[SINK] Count: %d | Rate: %.1f/s | Overall: %.1f/s | Time: %.1fs%n", 
                    count, rate, overall, total/1000.0);
            
            if (writer != null) {
                writer.printf("%d,%d,%.1f,%.1f%n", now, count, rate, total/1000.0);
                writer.flush();
            }
            
            lastReportTime = now;
        }
    }
    

    // called when the sink is closed in the Flink job
    // it prints the final stats and closes the output file
    // it calculates the min, max, and average rates for each interval
    public void close() throws Exception {
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        double overall = (1000.0 * count) / totalTime;
        
        // stats
        double min = Double.MAX_VALUE;
        double max = 0;
        double sum = 0;
        
        for (Double r : rates) {
            min = Math.min(min, r);
            max = Math.max(max, r);
            sum += r;
        }
        
        double avg = sum / rates.size();
        

        System.out.println("TEST COMPLETED: " + testName);
        System.out.println("Total events: " + count);
        System.out.println("Duration: " + (totalTime / 1000.0) + " seconds");
        System.out.println("Overall rate: " + String.format("%.1f", overall) + " events/second");
        System.out.println("Min rate: " + String.format("%.1f", min == Double.MAX_VALUE ? 0 : min));
        System.out.println("Max rate: " + String.format("%.1f", max));
        System.out.println("Avg rate: " + String.format("%.1f", avg));
        
        // write to file
        if (writer != null) {
            writer.println("\nSUMMARY");
            writer.println("Test: " + testName);
            writer.println("Events: " + count);
            writer.println("Time: " + (totalTime / 1000.0) + " seconds");
            writer.println("Rate: " + String.format("%.1f", overall) + " events/second");
            writer.println("Min: " + String.format("%.1f", min == Double.MAX_VALUE ? 0 : min));
            writer.println("Max: " + String.format("%.1f", max));
            writer.println("Avg: " + String.format("%.1f", avg));
            writer.close();
        }
    }
}
