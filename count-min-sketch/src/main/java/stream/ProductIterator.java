package stream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

public class ProductIterator implements Iterator<Product>, Serializable {

    public ProductIterator() throws IOException {
        InputStream stream = getClass().getResourceAsStream("/amazon.csv");
        if (stream == null) {
            throw new IOException("Ensure amazon.csv is in resources ");
        }
        Reader in = new InputStreamReader(stream);
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(in);
        for (CSVRecord record : records) {
            System.out.println(record);
        }
    }

    public boolean hasNext() {
        return true;
    }

    public Product next() {
        return null;
    }
}
