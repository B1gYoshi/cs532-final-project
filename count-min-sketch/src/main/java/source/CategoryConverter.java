package source;

import com.opencsv.bean.AbstractBeanField;

public class CategoryConverter extends AbstractBeanField<String, String> {
    @Override
    protected Object convert(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }

        // Get the most specific category
        String[] parts = value.split("\\|");
        String last = parts[parts.length - 1]
            .replaceAll("([A-Z])(?=[A-Z][a-z])", "$1 ") // Handle acronyms, e.g. USBCharger -> USB Charger
            .replace("&", " & ")
            .replace(",", ", ");

        // Convert camel case to words joined by spaces
        String[] words = last.split("(?<=[a-z])(?=[A-Z])");
        return String.join(" ", words);
    }
}

