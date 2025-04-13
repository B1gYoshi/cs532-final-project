package distribution;

import org.apache.flink.configuration.YamlParserUtils;
import java.io.File;
import java.util.*;

public class CustomDistribution implements Distribution<String> {
    private final List<String> implicit;
    private final HashMap<String, Double> explicit;
    private final Random random;

    public CustomDistribution() {
        explicit = new HashMap<>();
        implicit = new ArrayList<>();
        random = new Random();
    }

    public CustomDistribution loadYaml(Collection<String> domain, File file) {
        try {
            // Load weights from config file
            explicit.clear();
            Map<String, Object> yaml = YamlParserUtils.loadYamlFile(file);
            for (String category : YamlParserUtils.loadYamlFile(file).keySet()) {
                explicit.put(category, (Double)yaml.get(category));
            }

            // Separate elements with unspecified weight
            implicit.clear();
            for (String category : domain) {
                if (!explicit.containsKey(category)) {
                    implicit.add(category);
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return this;
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
