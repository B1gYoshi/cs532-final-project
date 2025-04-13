package stream;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;
import com.opencsv.bean.CsvToBeanBuilder;
import distribution.CustomDistribution;
import distribution.Distribution;

public class PurchaseGenerator implements Serializable {
    private final Map<String, List<Purchase>> groups;
    private final Distribution<String> categoryDist;
    private final Random random;

    public PurchaseGenerator() {
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

            // Setup category distribution
            File file = new File(weightsUrl.getPath());
            categoryDist = new CustomDistribution().fromYaml(groups.keySet(), file);
            random = new Random();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Purchase next() {
        String category = categoryDist.sample();
        List<Purchase> group = groups.get(category);
        return group.get(random.nextInt(group.size()));
    }
}
