package countminsketch;

import org.apache.commons.codec.digest.MurmurHash3;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;


// gets the merged CMS for all of the partitions
// Somehow needs to figure out the most popular items
public class CMSAnalysis extends ProcessFunction<CMSMergedResult, PurchaseAlert> {

    private final int M;

    public CMSAnalysis (int M) {
        this.M = M;
    }

    @Override
    public void processElement(CMSMergedResult cmsMergedResult, ProcessFunction<CMSMergedResult, PurchaseAlert>.Context context, Collector<PurchaseAlert> collector) throws Exception {
        PurchaseAlert alert = new PurchaseAlert();

        String[] categories = {"Computer", "Home&Kitchen", "category3", "category4"};
        String resultString = "";
        int[] cmsMergedArr = cmsMergedResult.getCmsArrayMerged();

        for (String categoryStr : categories) {
            byte[] categoryBytes =categoryStr.getBytes();
            int hash = MurmurHash3.hash32x86(categoryBytes, 0, categoryBytes.length, 0);
            int cmsKey = Math.floorMod(hash, this.M);
            resultString += categoryStr + ": " + cmsMergedArr[cmsKey] + ". ";
        }

        alert.setMessage("The counts are as follows. " + resultString);

        collector.collect(alert);
    }
}
