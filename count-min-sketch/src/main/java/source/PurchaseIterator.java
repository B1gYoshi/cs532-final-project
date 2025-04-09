package source;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import com.opencsv.bean.CsvToBeanBuilder;

public class PurchaseIterator implements Iterator<Purchase>, Serializable {
    private final List<Purchase> purchases;
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
            purchases = new CsvToBeanBuilder<Purchase>(in)
                .withType(Purchase.class)
                .build()
                .parse();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean hasNext() {
        return true;
    }

    public Purchase next() {
        // TODO: Add logic to change distribution by product category
        int index = random.nextInt(purchases.size());
        return purchases.get(index).copy();
    }
}
