package countminsketch;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.commons.codec.digest.MurmurHash3;
import stream.Purchase;

import java.util.*;
import java.util.stream.Collectors;

public class CountMinSketch extends ProcessWindowFunction<
        Tuple2<Integer, Purchase>,
        CMSResult,
        Integer,
        TimeWindow> {

    private final int M;
    private final int K;

    public CountMinSketch (int M, int K) {
        this.M = M;
        this.K = K;
    }

    private int hashCategory(String category) {
        byte[] bytes = category.getBytes();
        int hash = MurmurHash3.hash32x86(bytes, 0, bytes.length, 0);

        return Math.floorMod(hash, this.M);
    }

    @Override
    public void process(Integer key, ProcessWindowFunction<Tuple2<Integer, Purchase>, CMSResult, Integer, TimeWindow>.Context context, Iterable<Tuple2<Integer, Purchase>> elements, Collector<CMSResult> collector) throws Exception {

        int[] cmsArray = new int[this.M];
        TreeSet<TreeSetEntry> topKCategories = new TreeSet<>();

        for (Tuple2<Integer, Purchase> purchase : elements) {
            String category = purchase.f1.getCategory();

            int cmsKey = hashCategory(category);
            cmsArray[cmsKey] += 1;

            TreeSetEntry newEntry = new TreeSetEntry(category, cmsArray[cmsKey]);

            topKCategories.remove(newEntry);
            topKCategories.add(newEntry);

            if (topKCategories.size() > K) {
                topKCategories.pollFirst();
            }

        }

        Set<String> topKCategoriesSet = topKCategories.stream()
                .map(TreeSetEntry::getCategory)
                .collect(Collectors.toSet());

        collector.collect(new CMSResult(cmsArray, context.window().getStart(), topKCategoriesSet));
    }
}

class TreeSetEntry implements Comparable<TreeSetEntry> {
    private final String category;
    private final int estimate;

    TreeSetEntry(String category, int estimate) {
        this.category = category;
        this.estimate = estimate;
    }

    public String getCategory() {
        return category;
    }

    @Override
    public int compareTo(TreeSetEntry other) {
        int cmp = this.estimate - other.estimate;
        if (cmp == 0) {
            cmp = this.category.compareTo(other.category);
        }
        return cmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        TreeSetEntry other = (TreeSetEntry) o;
        return this.category.equals(other.category);
    }
}
