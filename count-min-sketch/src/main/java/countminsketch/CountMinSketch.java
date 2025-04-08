package countminsketch;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import java.util.Random;


public class CountMinSketch extends ProcessWindowFunction<
        Tuple2<Integer, Purchase>,  // IN
        PurchaseAlert,              // OUT
        Integer,                    // KEY
        TimeWindow> {               // WINDOW TYPE

    @Override
    public void process(Integer key, ProcessWindowFunction<Tuple2<Integer, Purchase>, PurchaseAlert, Integer, TimeWindow>.Context context, Iterable<Tuple2<Integer, Purchase>> elements, Collector<PurchaseAlert> collector) throws Exception {
        RuntimeContext runtimeContext = getRuntimeContext();
        int subtaskIndex = runtimeContext.getIndexOfThisSubtask();

        for (Tuple2<Integer, Purchase> transaction : elements) {
            PurchaseAlert alert = new PurchaseAlert();
            alert.setMessage("Key " + key +
                    " handled on subtask " + subtaskIndex +
                    ": " + transaction.f1.toString());

            collector.collect(alert);
        }
    }
}