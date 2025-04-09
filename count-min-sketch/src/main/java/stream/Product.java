package stream;

import org.apache.commons.lang3.builder.EqualsBuilder;
import java.util.Objects;

public class Product {
    public String product_id;
    public String product_name;
    public String category;
    public double discounted_price;
    public double actual_price;
    public double discount_percentage;
    public double rating;
    public int rating_count;
    public String about_product;
    public String user_id;
    public String user_name;
    public String review_id;
    public String review_title;
    public String review_content;
    public String img_link;
    public String product_link;

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
            .append(product_id, other.product_id)
            .append(product_name, other.product_name)
            .append(category, other.category)
            .append(discounted_price, other.discounted_price)
            .append(actual_price, other.actual_price)
            .append(discount_percentage, other.discount_percentage)
            .append(rating, other.rating)
            .append(rating_count, other.rating_count)
            .append(about_product, other.about_product)
            .append(user_id, other.user_id)
            .append(user_name, other.user_name)
            .append(review_id, other.review_id)
            .append(review_title, other.review_title)
            .append(review_content, other.review_content)
            .append(img_link, other.img_link)
            .append(product_link, other.product_link)
            .isEquals();
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            product_id,
            product_name,
            category,
            discounted_price,
            actual_price,
            discount_percentage,
            rating,
            rating_count,
            about_product,
            user_id,
            user_name,
            review_id,
            review_title,
            review_content,
            img_link,
            product_link
        );
    }
}
