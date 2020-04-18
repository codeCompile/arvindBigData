package guru.learningjournal.kafka.examples.datagenerator;

import com.fasterxml.jackson.databind.ObjectMapper;
import guru.learningjournal.kafka.examples.types.DeliveryAddress;

import java.io.File;
import java.util.Random;

class AddressGenerator {
    private static final AddressGenerator ourInstance = new AddressGenerator();
    private final Random random;

    private DeliveryAddress[] addresses;

    private int getIndex() {
        return random.nextInt(100);
    }

    static AddressGenerator getInstance() {
        return ourInstance;
    }

    private AddressGenerator() {
        final String DATAFILE = "src/main/resources/data/address.json";
        final ObjectMapper mapper;
        random = new Random();
        mapper = new ObjectMapper();
        File file = new File(
                getClass().getClassLoader().getResource("data/address.json").getFile()
        );

        try {
            addresses = mapper.readValue(file, DeliveryAddress[].class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    DeliveryAddress getNextAddress() {
        return addresses[getIndex()];
    }

}
