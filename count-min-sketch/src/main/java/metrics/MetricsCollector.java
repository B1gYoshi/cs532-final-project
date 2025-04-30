package metrics;

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;

import java.io.Serializable;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;


public class MetricsCollector<T> extends RichMapFunction<T, T> implements Serializable {

    private static final long serialVersionUID = 1L;
    private transient Counter counter;
    private transient Meter meter;

    private transient SimpleWriter outputter;

    private transient RuntimeContext runtimeContext;

    // private transient long TOTAL_EVENTS = 1e6; // 1 million

    @Override
    public void open(OpenContext context) throws Exception {

        this.runtimeContext = getRuntimeContext();

        this.counter = getRuntimeContext().getMetricGroup().counter("numEvents");
        this.meter = getRuntimeContext().getMetricGroup().meter("eventsPerSecond", new MeterView(60));

        //optional: log to file, metrics should be in dashboard
        outputter = new SimpleWriter("metrics.log");


    }

    @Override
    public T map(T value) throws Exception {
        if (counter == null) {
            // doesn't normally run
            this.counter = getRuntimeContext().getMetricGroup().counter("numEvents");
            this.meter = getRuntimeContext().getMetricGroup().meter("eventsPerSecond", new MeterView(60));
        }
        counter.inc();
        meter.markEvent();

        // i think count is shared across all cores
        long count = counter.getCount();

        // not sure why but this is the only way to get coreID
        TaskInfo taskInfo = runtimeContext.getTaskInfo();
        int coreId = taskInfo.getIndexOfThisSubtask();

        coreId = runtimeContext.getTaskInfo().getIndexOfThisSubtask();




        //every 10 events, print count and rate (for every core)
        if (counter.getCount() % 10 == 0) {
            System.out.printf("Core: %d, Events: %d, EPS: %.2f", coreId, counter.getCount(), meter.getRate());


            // outputter.log(String.format("Events: %d, EPS: %.2f", count, meter.getRate()));
            outputter.log(String.format("Core: %d, Events: %d, EPS: %.2f", coreId, counter.getCount(), meter.getRate()));
        }


        //no point if no coreID:

        // if (count == 0) {
        //     startTime = System.currentTimeMillis();
        // } else if (count == TOTAL_EVENTS) {
        //     endTime = System.currentTimeMillis();
        //     long duration = endTime - startTime;
        //     System.out.printf("Processed 1,000,000 events in %d ms%n", duration);
        // }



        //latency tracking: requires timestamp in Purchase Object TODO
        // long now = System.currentTimeMillis();
        // long latency = now - value.getTimestamp();
        // latencies.add(latency);
        return value;
    }

    @Override
    public void close() throws Exception {
        outputter.close();
    }
}
