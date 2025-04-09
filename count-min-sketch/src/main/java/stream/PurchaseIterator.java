package stream;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import com.opencsv.bean.CsvToBeanBuilder;

public class PurchaseIterator implements Iterator<Purchase>, Serializable {
    final private List<Purchase> purchases;

    public PurchaseIterator() throws IOException {
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

    public boolean hasNext() {
        return true;
    }

    public Purchase next() {
        return purchases.get(0);
    }
}
