package countminsketch;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.commons.codec.digest.MurmurHash3;

public class CountMinSketch extends ProcessWindowFunction<
        Tuple2<Integer, Purchase>,
        CMSResult,
        Integer,
        TimeWindow> {

    private final int M;

    public CountMinSketch (int M) {
        this.M = M;
    }

    @Override
    public void process(Integer key, ProcessWindowFunction<Tuple2<Integer, Purchase>, CMSResult, Integer, TimeWindow>.Context context, Iterable<Tuple2<Integer, Purchase>> elements, Collector<CMSResult> collector) throws Exception {
        RuntimeContext runtimeContext = getRuntimeContext();
        int subtaskIndex = runtimeContext.getIndexOfThisSubtask();

        int[] cmsArray = new int[this.M];

        for (Tuple2<Integer, Purchase> purchase : elements) {
            byte[] bytes = purchase.f1.getCategory().getBytes();
            int hash = MurmurHash3.hash32x86(bytes, 0, bytes.length, 0);
            int cmsKey = Math.floorMod(hash, this.M);

            cmsArray[cmsKey] += 1;
        }

        collector.collect(new CMSResult(cmsArray, context.window().getStart()));
    }
}