package tn.utm.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

public class CommandeProducer {
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

                Commande cmd = new Commande(
                        "cmd-" + i,
                        "2026-05-02",
                        Arrays.asList("articleA", "articleB"),
                        100 + i * 50
                );

                String json = mapper.writeValueAsString(cmd);

                producer.send(new ProducerRecord<>("commandes-json", cmd.getId(), json));

                System.out.println("Envoyé : " + json);
            }

            producer.flush();
        }
    }
}
