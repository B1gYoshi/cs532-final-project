package countminsketch;

import org.apache.commons.codec.digest.MurmurHash3;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Set;


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

        Set<String> categories = cmsMergedResult.getPopularCategoriesCandidates();
        String popularCategory = "";
        int popularCategoryCount = -1;
        int[] cmsMergedArr = cmsMergedResult.getCmsArrayMerged();

        for (String categoryStr : categories) {
            byte[] categoryBytes =categoryStr.getBytes();
            int hash = MurmurHash3.hash32x86(categoryBytes, 0, categoryBytes.length, 0);
            int cmsKey = Math.floorMod(hash, this.M);

            if (cmsMergedArr[cmsKey] > popularCategoryCount) {
                popularCategoryCount = cmsMergedArr[cmsKey];
                popularCategory = categoryStr;
            }
        }

        alert.setMessage("The most popular category is " + popularCategory + " with count " + popularCategoryCount + ".");

        collector.collect(alert);
    }
}
