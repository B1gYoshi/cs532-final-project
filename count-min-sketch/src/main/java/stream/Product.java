package stream;

import com.opencsv.bean.CsvBindByName;
import org.apache.commons.lang3.builder.EqualsBuilder;
import java.util.Objects;


public class Product {
    @CsvBindByName(column = "product_id") public String productId;
    @CsvBindByName(column = "product_name") public String productName;
    public String category;

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || this.getClass() != object.getClass()) {
            return false;
        }
        Product other = (Product)object;
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
}
