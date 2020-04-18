package guru.learningjournal.kafka.examples.datagenerator;

import com.fasterxml.jackson.databind.ObjectMapper;
import guru.learningjournal.kafka.examples.types.LineItem;

import java.io.File;
import java.util.Random;

class ProductGenerator {
    private static final ProductGenerator ourInstance = new ProductGenerator();
    private final Random random;
    private final Random qty;
    private final LineItem[] products;

    static ProductGenerator getInstance() {
        return ourInstance;
    }

    private ProductGenerator() {
        String DATAFILE = "src/main/resources/data/products.json";
        ObjectMapper mapper = new ObjectMapper();
        random = new Random();
        qty = new Random();
        File file = new File(
                getClass().getClassLoader().getResource("data/products.json").getFile()
        );

        try {
            products = mapper.readValue(file, LineItem[].class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int getIndex() {
        return random.nextInt(100);
    }

    private int getQuantity() {
        return qty.nextInt(2) + 1;
    }

    LineItem getNextProduct() {
        LineItem lineItem = products[getIndex()];
        lineItem.setItemQty(getQuantity());
        lineItem.setTotalValue(lineItem.getItemPrice() * lineItem.getItemQty());
        return lineItem;
    }
}
