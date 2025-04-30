package cms1D;

import java.util.Objects;

// Detailed alert class based off of the Alert class from the flink walkthrough
// We essentially copy and pasted the whole class and added the message variable and the related getter/setter
public class PurchaseAlert {
    private long id;
    private String message;

    public PurchaseAlert() {
    }

    public long getId() {
        return this.id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            PurchaseAlert alert = (PurchaseAlert)o;
            return this.id == alert.id;
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.id});
    }

    public String getMessage() {
        return this.message;
    }

    // added setMessage in order to include all relevant information in alerts, with id, timestamp, zipcode, and amount
    public void setMessage(String message) {
        this.message = message;
    }

    public String toString() {
        return this.message;
    }
}
