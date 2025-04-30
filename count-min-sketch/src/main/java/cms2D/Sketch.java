package cms2D;

import org.apache.commons.codec.digest.MurmurHash3;

public class Sketch {
    private final int width;     // Length of estimate arrays
    private final int depth;     // Number of arrays and hash functions
    private long total;          // The total number of items seen
    private final int[] seeds;
    private final int[][] sketch;

    public Sketch(int width, int depth) {
        this.width = width;
        this.depth = depth;
        this.total = 0;
        this.sketch = new int[depth][width];
        this.seeds = new int[depth];

        // Initialize static seeds
        for (int i = 0; i < depth; i++) {
            seeds[i] = i;
        }
    }

    public int update(String key) {
        byte[] bytes = key.getBytes();
        int min = Integer.MAX_VALUE;

        // Increment corresponding estimates and return new min
        for (int i = 0; i < depth; i++) {
            int hash = MurmurHash3.hash32x86(bytes, 0, bytes.length, seeds[i]);
            int j = Math.floorMod(hash, width);
            sketch[i][j] += 1;
            min = Math.min(min, sketch[i][j]);
        }

        total++;
        return min;
    }

    public int estimate(String key) {
        byte[] bytes = key.getBytes();
        int min = Integer.MAX_VALUE;

        // Return minimum among corresponding entries
        for (int i = 0; i < depth; i++) {
            int hash = MurmurHash3.hash32x86(bytes, 0, bytes.length, seeds[i]);
            int j = Math.floorMod(hash, width);
            min = Math.min(min, sketch[i][j]);
        }
        return min;
    }

    public void merge(Sketch other) {
        // Add corresponding entries
        for (int i = 0; i < depth; i++) {
            for (int j = 0; j < width; j++) {
                sketch[i][j] += other.sketch[i][j];
            }
        }
    }

    public long getTotal() {
        return total;
    }
}