package cms1D;

import org.apache.flink.shaded.guava32.com.google.common.collect.Sets;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

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
        Set<String> categoriesUnion = new HashSet<>();

        for (CMSResult cmsResult : cmsResults) {

            categoriesUnion = new HashSet<>(Sets.union(categoriesUnion, cmsResult.getTopKCategories()));

            int[] arr = cmsResult.getCmsArray();
            for (int i = 0; i < this.M; i++) {
                mergedCMS[i] += arr[i];
            }
        }

        collector.collect(new CMSMergedResult(mergedCMS, categoriesUnion));


    }
}
