package countminsketch;

import java.util.Objects;

// DetailedTransaction class based off of the Transaction class from the flink walkthrough
// We essentially copy and pasted the whole class and added the zip code variable and the getter/setter
// We also modified the toString function
public class Purchase {

    private String productId;
    private String productName;
    private String category;


    // new class constructor to set the transaction zipCode
    public Purchase(String productId, String productName, String category) {
        this.productId = productId;
        this.productName = productName;
        this.category = category;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId (String productId) {
        this.productId = productId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName (String productName) {
        this.productName = productName;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory (String category) {
        this.category = category;
    }

    @Override
    public int hashCode() {
        return Objects.hash(productId, productName, category);
    }

    // modified toString method to include zipCode in output
    @Override
    public String toString() {
        return "Purchase{" + "productId=" + this.productId + ", productName=" + this.productName + ", category=" + this.category + '}';
    }
}
