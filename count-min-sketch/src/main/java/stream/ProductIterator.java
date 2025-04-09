package stream;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import com.opencsv.bean.CsvToBeanBuilder;

public class ProductIterator implements Iterator<Product>, Serializable {

    public ProductIterator() throws IOException {
        // Open CSV file
        InputStream stream = getClass().getResourceAsStream("/amazon.csv");
        if (stream == null) {
            throw new IOException("Missing amazon.csv in resources folder");
        }

        // Parse CSV file into POJOs
        Reader in = new InputStreamReader(stream);
        List<Product> products = new CsvToBeanBuilder<Product>(in)
            .withType(Product.class)
            .build()
            .parse();

        for (Product product : products) {
            System.out.println(product.productName);
        }
    }

    public boolean hasNext() {
        return true;
    }

    public Product next() {
        return null;
    }
}
