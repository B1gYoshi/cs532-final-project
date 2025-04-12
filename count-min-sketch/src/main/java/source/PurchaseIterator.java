package source;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;
import com.opencsv.bean.CsvToBeanBuilder;
import org.apache.flink.configuration.YamlParserUtils;

public class PurchaseIterator implements Iterator<Purchase>, Serializable {
    private final Map<String, Double> weights;
    private final Map<String, List<Purchase>> groups;
    private final Random random;

    public PurchaseIterator() {
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

            // Parse weights assigned in yaml file
            weights = new HashMap<>();
            File weightsFile = new File(weightsUrl.getPath());
            Map<String, Object> yaml = YamlParserUtils.loadYamlFile(weightsFile);
            Double assigned = 0.0;
            for (String category : yaml.keySet()) {
                Double weight = (Double)yaml.get(category);
                assigned += weight;
                weights.put(category, weight);
            }

            // Set remaining unassigned weights uniformly
            Double uniform = (1.0 - assigned) / (groups.size() - yaml.size());
            for (String category : groups.keySet()) {
                if (!yaml.containsKey(category)) {
                    weights.put(category, uniform);
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Purchase next() {
        return null;
    }

    public boolean hasNext() {
        return true;
    }
}
