package metrics;

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;

import java.io.Serializable;

public class MetricsCollector<T> extends RichMapFunction<T, T> implements Serializable {

    private static final long serialVersionUID = 1L;
    private transient Counter counter;
    private transient Meter meter;

    private transient long startTime = -1;
    private transient long endTime = -1;
    private transient long lastCheckpointTime = -1;
    private static final long CHECKPOINT_INTERVAL = 100_000;

    private static final long TOTAL_EVENTS = 10_000_000L;

    private transient SimpleWriter outputter;

    private transient RuntimeContext runtimeContext;

    @Override
    public void open(OpenContext context) throws Exception {
        this.runtimeContext = getRuntimeContext();

        // counter and meter persist across parallel instances
        this.counter = runtimeContext.getMetricGroup().counter("numEvents");
        this.meter = runtimeContext.getMetricGroup().meter("eventsPerSecond", new MeterView(1));

        int totalCores = runtimeContext.getTaskInfo().getNumberOfParallelSubtasks();
        outputter = new SimpleWriter(String.format("metrics-logs/metrics-%d-cores.csv", totalCores));
        
        int coreId = runtimeContext.getTaskInfo().getIndexOfThisSubtask();
        if (coreId == 0) {
            outputter.log("Core_ID,Events,EPS,time");
        }

        meter.markEvent();
        lastCheckpointTime = System.currentTimeMillis();
        startTime = lastCheckpointTime;
    }

    @Override
    public T map(T value) throws Exception {
        counter.inc();

        long count = counter.getCount();
        int coreId = runtimeContext.getTaskInfo().getIndexOfThisSubtask();

        if (count % CHECKPOINT_INTERVAL == 0) {
            long now = System.currentTimeMillis();
            long elapsedMillis = now - lastCheckpointTime;

            if (elapsedMillis > 0) {
                double eps = (CHECKPOINT_INTERVAL * 1000.0) / elapsedMillis;
                long timeElapsed = now - startTime;

                outputter.log(String.format("%d,%d,%.2f,%d", coreId, count, eps, timeElapsed));

                if (coreId == 0) {
                    System.out.printf("Core: %d, Events: %d, EPS: %.2f, Time: %d\n", coreId, count, eps, timeElapsed);
                }
            }

            lastCheckpointTime = now;
        }

        return value;
    }

    @Override
    public void close() throws Exception {

        endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        int coreId = runtimeContext.getTaskInfo().getIndexOfThisSubtask();

        outputter.log(String.format("%d,FINISH,%dms", coreId, duration));
        outputter.close();
    }
}
