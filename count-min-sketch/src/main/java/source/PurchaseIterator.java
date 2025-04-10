package source;

import java.io.*;
import java.util.*;
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
            Reader in = new InputStreamReader(stream);
            Iterator<Purchase> iter = new CsvToBeanBuilder<Purchase>(in)
                .withType(Purchase.class)
                .build()
                .iterator();

            // Group purchases by category
            groups = new HashMap<String, List<Purchase>>();
            while (iter.hasNext()) {
                Purchase purchase = iter.next();
                List<Purchase> list = groups.computeIfAbsent(purchase.getCategory(), (key) -> new ArrayList<>());
                list.add(purchase);
            }

            // Used to randomly choose category group
            categories = new ArrayList<>(groups.keySet());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean hasNext() {
        return true;
    }

    public Purchase next() {
        // TODO: Add logic to change distribution by product category,
        // TODO: Should be able to manually configure some categories, and the rest are uniform
        // TODO: Figure out if object should have 1 or multiple categories (split category along `|` and into capitalized words between those)

        // Choose a category group, then a purchase in it
        String category = categories.get(random.nextInt(categories.size()));
        List<Purchase> group = groups.get(category);
        return group.get(random.nextInt(group.size()));
    }
}
