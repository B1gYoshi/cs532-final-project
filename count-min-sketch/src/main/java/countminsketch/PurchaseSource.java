package countminsketch;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class PurchaseSource implements SourceFunction<Purchase> {
    private volatile boolean running = true;
    private final String csvFilePath;
    private final long delayMs;//milliseconds

    public PurchaseSource(String csvFilePath, long delayMs) {
        this.csvFilePath = csvFilePath;
        this.delayMs = delayMs;
    }

    @Override
    public void run(SourceContext<Purchase> sourceContext) throws Exception {
        List<Purchase> purchases = loadPurchasesFromCSV();
        
        int index = 0;
        while (running) {
            sourceContext.collect(purchases.get(index));
            index = (index + 1) % purchases.size();
            Thread.sleep(delayMs);
        }
    }

    private List<Purchase> loadPurchasesFromCSV() {
        List<Purchase> purchases = new ArrayList<>();
    
        Reader reader = new FileReader(csvFilePath);
        
        // commons csv for parsing
        CSVParser parser = CSVFormat.DEFAULT
            .withFirstRecordAsHeader()
            .withIgnoreHeaderCase()
            .withTrim()
            .parse(reader);
        
        for (CSVRecord record : parser) {

            //creates purchases here

            //TODO edit fields

            String productId = getValueOrDefault(record, "product_id");
            String productName = getValueOrDefault(record, "product_name");
            String category = getValueOrDefault(record, "category");
            
            purchases.add(new Purchase(productId, productName, category));
        }
        
        reader.close();
        
        return purchases;
    }
    
    private String getValue(CSVRecord record, String columnName) {

        //TODO: adapt for missing values if needed
        return record.get(columnName);
    }
    
    // for debugging
    private List<Purchase> getDefaultPurchases() {
        List<Purchase> ret = new ArrayList<>();
        ret.add(new Purchase("1", "prod1", "Computer"));
        ret.add(new Purchase("2", "prod2", "Home&Kitchen"));
        ret.add(new Purchase("3", "prod3", "Electronics"));
        ret.add(new Purchase("4", "prod4", "Clothing"));
        return ret;
    }

    @Override
    public void stopRunning() {
        running = false;
    }
}
