package countminsketch;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CMSMerge extends ProcessWindowFunction<
        CMSResult,
        CMSMergedResult,
        Long,
        TimeWindow>
{

    private final int M;

    public CMSMerge (int M) {
        this.M = M;
    }

    @Override
    public void process(Long key, ProcessWindowFunction<CMSResult, CMSMergedResult, Long, TimeWindow>.Context context, Iterable<CMSResult> cmsResults, Collector<CMSMergedResult> collector) throws Exception {
        int[] mergedCMS = new int[M];

        for (CMSResult cmsResult : cmsResults) {
//            PurchaseAlert alert = new PurchaseAlert();
//
//            RuntimeContext runtimeContext = getRuntimeContext();
//            int subtaskIndex = runtimeContext.getIndexOfThisSubtask();
//            alert.setMessage("Timestamp " + cmsResult.getWindowTimestamp() +
//                    " handled on subtask " + subtaskIndex + " in tumbling window starting at "
//                    + context.window().getStart());
//
//            collector.collect(alert);
            int[] arr = cmsResult.getCmsArray();
            for (int i = 0; i < this.M; i++) {
                mergedCMS[i] += arr[i];
            }
        }

        collector.collect(new CMSMergedResult(mergedCMS));


    }
}
