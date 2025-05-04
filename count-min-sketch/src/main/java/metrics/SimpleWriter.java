package metrics;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class SimpleWriter {

    private final PrintWriter writer;

    public SimpleWriter(String filename) throws IOException {
        this.writer = new PrintWriter(new FileWriter(filename, true)); // true is append mode
    }

    public synchronized void log(String message) {
        writer.println(message);
        writer.flush();
    }

    public void close() {
        writer.close();
    }
}
