package source;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import com.opencsv.bean.CsvToBeanBuilder;

public class PurchaseIterator implements Iterator<Purchase>, Serializable {
    private final List<String> categories;
    private final Map<String, List<Purchase>> groups;
    private final Random random;

    public PurchaseIterator() {
        random = new Random();
        try {
            // Open CSV file
            InputStream stream = getClass().getResourceAsStream("/amazon.csv");
            if (stream == null) {
                throw new IOException("Missing amazon.csv in resources folder");
            }

            // Parse CSV file rows into POJOs
            Reader reader = new InputStreamReader(stream);
            List<Purchase> beans = new CsvToBeanBuilder<Purchase>(reader)
                .withType(Purchase.class)
                .build()
                .parse();

            // Group purchases by category
            groups = beans.stream().collect(Collectors.groupingBy(Purchase::getCategory));
            categories = new ArrayList<>(groups.keySet());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Purchase next() {
        // TODO: Add logic to change distribution by product category,
        // TODO: Should be able to manually configure some categories, and the rest are uniform

        // Choose a category group, then a purchase in it
        String category = categories.get(random.nextInt(categories.size()));
        List<Purchase> group = groups.get(category);
        return group.get(random.nextInt(group.size())).copy();
    }

    public boolean hasNext() {
        return true;
    }
}
