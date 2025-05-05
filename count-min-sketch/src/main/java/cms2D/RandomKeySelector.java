package cms2D;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import stream.Purchase;
import java.util.Random;

public class RandomKeySelector extends RichMapFunction<Purchase, Tuple2<Integer, Purchase>> {
    private final int NUM_CORES;
    private final Random random;

    public RandomKeySelector (int NUM_CORES) {
        this.NUM_CORES = NUM_CORES;
        random = new Random();
    }

    @Override
    public Tuple2<Integer, Purchase> map(Purchase purchase) throws Exception {
        return Tuple2.of(random.nextInt(NUM_CORES), purchase);
    }
}
