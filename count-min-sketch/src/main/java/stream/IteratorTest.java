package stream;

import java.io.IOException;

public class IteratorTest {

    public static void main(String[] args) throws IOException {
        PurchaseIterator it = new PurchaseIterator();
        System.out.println(it.next());
    }
}
