package stream;

import com.opencsv.bean.CsvBindByName;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Objects;


public class Purchase {
    @CsvBindByName(column = "product_id")
    private String productId;

    @CsvBindByName(column = "product_name")
    private String productName;

    @CsvBindByName(column = "category")
    private String category;

    public Purchase() {}

    public Purchase(String productId, String productName, String category) {
        this.productId = productId;
        this.productName = productName;
        this.category = category;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Purchase copy() {
        return new Purchase(productId, productName, category);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || this.getClass() != object.getClass()) {
            return false;
        }
        Purchase other = (Purchase)object;
        return new EqualsBuilder()
            .append(productId, other.productId)
            .append(productName, other.productName)
            .append(category, other.category)
            .isEquals();
    }

    @Override
    public int hashCode() {
        return Objects.hash(productId, productName, category);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).
            append("productId", productId).
            append("productName", productName).
            append("category", category).
            toString();
    }
}
