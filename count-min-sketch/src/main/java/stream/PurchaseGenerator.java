package stream;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;
import com.opencsv.bean.CsvToBeanBuilder;
import org.apache.flink.configuration.YamlParserUtils;

public class PurchaseGenerator implements Serializable {
    private final double implicitWeight;
    private final Map<String, Double> explicitWeights;
    private final Map<String, List<Purchase>> groups;
    private final Random random;

    public PurchaseGenerator() {
        random = new Random();
        try {
            // Load resource files
            URL weightsUrl = getClass().getResource("/weights.yaml");
            InputStream csvStream = getClass().getResourceAsStream("/amazon.csv");
            if (weightsUrl == null || csvStream == null) {
                throw new IOException("Missing files in resources folder");
            }

            // Parse CSV rows and group by category
            Reader reader = new InputStreamReader(csvStream);
            groups = new CsvToBeanBuilder<Purchase>(reader)
                .withType(Purchase.class)
                .build()
                .parse()
                .stream()
                .collect(Collectors.groupingBy(Purchase::getCategory));

            // Parse category weights explicitly assigned in weights.yaml
            explicitWeights = new HashMap<>();
            File weightsFile = new File(weightsUrl.getPath());
            Map<String, Object> yaml = YamlParserUtils.loadYamlFile(weightsFile);
            Double total = 0.0;
            for (String category : yaml.keySet()) {
                Double weight = (Double)yaml.get(category);
                total += weight;
                explicitWeights.put(category, weight);
            }

            // Uniform weight used for the remaining categories
            implicitWeight = (1.0 - total) / (groups.size() - yaml.size());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Purchase next() {
        // TODO: Most iterations just subtract the uniform weight. Faster to iterate over assigned weights, then do some static calculation if it's not one of those
        double threshold = random.nextDouble();
        for (String category : groups.keySet()) {
            double weight = explicitWeights.getOrDefault(category, implicitWeight);
            threshold -= weight;
            if (threshold <= 0) {
                List<Purchase> group = groups.get(category);
                return group.get(random.nextInt(group.size()));
            }
        }
        return null; // Rounding error could trigger this but not sure
    }
}
