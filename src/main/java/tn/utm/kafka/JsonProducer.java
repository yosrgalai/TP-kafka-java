package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class JsonProducer {
    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        ObjectMapper mapper = new ObjectMapper();

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {

            for (int i = 1; i <= 5; i++) {

                Vente vente = new Vente(
                        "client-" + (i % 3),
                        100 + i * 50,
                        "Tunis"
                );

                String json = mapper.writeValueAsString(vente);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>("ventes", vente.getIdClient(), json);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("✓ JSON envoyé : " + json);
                    }
                });
            }

            producer.flush();
        }
    }
}
