package cms;

import org.apache.commons.codec.digest.MurmurHash3;
import java.util.*;

public class Sketch {
    private final int width;                // Length of estimate arrays
    private final int depth;                // Number of arrays and hash functions
    private final int maxHotKeys;
    private final int[] seeds;
    private final int[][] sketch;
    private final TreeSet<HotKey> hotKeys;  // Top k keys by estimation

    public Sketch(int width, int depth, int maxHotKeys) {
        this.width = width;
        this.depth = depth;
        this.maxHotKeys = maxHotKeys;
        this.sketch = new int[depth][width];
        this.seeds = new int[depth];
        this.hotKeys = new TreeSet<>();

        // Initialize static seeds
        for (int i = 0; i < depth; i++) {
            seeds[i] = i;
        }
    }

    public List<HotKey> getHotKeys() {
        return new ArrayList<>(hotKeys);
    }

    public void update(String key) {
        byte[] bytes = key.getBytes();
        int min = Integer.MAX_VALUE;

        // Increment corresponding estimates
        for (int i = 0; i < depth; i++) {
            int hash = MurmurHash3.hash32x86(bytes, 0, bytes.length, seeds[i]);
            int j = Math.floorMod(hash, width);
            sketch[i][j] += 1;
            min = Math.min(min, sketch[i][j]);
        }

        // Update potential hot key
        HotKey hotKey = new HotKey(key, min);
        hotKeys.remove(hotKey);
        hotKeys.add(hotKey);
        if (hotKeys.size() > maxHotKeys) {
            hotKeys.pollFirst();
        }
    }
}