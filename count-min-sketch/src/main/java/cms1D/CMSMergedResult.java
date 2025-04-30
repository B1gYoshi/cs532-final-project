package cms1D;

import java.util.Set;

public class CMSMergedResult {

    private final int[] cmsArrayMerged;
    private final Set<String> popularCategoriesCandidates;

    public CMSMergedResult (int[] cmsArrayMerged, Set<String> popularCategoriesCandidates) {
        this.cmsArrayMerged = cmsArrayMerged;
        this.popularCategoriesCandidates = popularCategoriesCandidates;
    }

    public int[] getCmsArrayMerged() {
        return cmsArrayMerged;
    }

    public Set<String> getPopularCategoriesCandidates() {
        return popularCategoriesCandidates;
    }
}
