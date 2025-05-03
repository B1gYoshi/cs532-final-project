package distribution;

import org.apache.flink.configuration.YamlParserUtils;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;

public class CustomDistribution implements Distribution<String> {
    // Configure to desired weights file
    private final String WEIGHTS_FILE = "/weights.yaml";

    private final List<String> implicit;
    private final HashMap<String, Double> explicit;
    private final Random random;

    public CustomDistribution(Collection<String> domain) throws Exception {
        // Load file with custom weights
        URL weightsUrl = getClass().getResource(WEIGHTS_FILE);
        if (weightsUrl == null) {
            throw new IOException("Missing weights.yaml in resources folder");
        }

        // Parse specified weights
        explicit = new HashMap<>();
        File file = new File(weightsUrl.getPath());
        Map<String, Object> config = YamlParserUtils.loadYamlFile(file);
        for (String category : config.keySet()) {
            explicit.put(category, (Double)config.get(category));
        }

        // Separate elements with unspecified weight
        implicit = new ArrayList<>();
        for (String category : domain) {
            if (!explicit.containsKey(category)) {
                implicit.add(category);
            }
        }

        random = new Random();
    }

    public String sample() {
        double roll = random.nextDouble();
        for (String category : explicit.keySet()) {
            roll -= explicit.get(category);
            if (roll <= 0) {
                return category;
            }
        }
        return implicit.get(random.nextInt(implicit.size())); // Uniform over unspecified elements
    }
}
