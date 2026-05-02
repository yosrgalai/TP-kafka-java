package tn.utm.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.*;

public class SimulateurCaisse {

    private static final String[] VILLES = {"Tunis", "Sousse", "Sfax", "Bizerte", "Gabes"};
    private static final String[] TYPES = {"VENTE", "RETOUR", "OUVERTURE"};

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        Producer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper mapper = new ObjectMapper();
        Random random = new Random();

        System.out.println("🚀 Simulation en cours...");

        while (true) {

            String ville = VILLES[random.nextInt(VILLES.length)];

            // Probabilités
            double p = random.nextDouble();
            String type;
            if (p < 0.7) type = "VENTE";
            else if (p < 0.8) type = "RETOUR";
            else type = "OUVERTURE";

            Map<String, Object> event = new HashMap<>();
            event.put("type", type);
            event.put("idCaisse", "CAISSE-" + ville.toUpperCase() + "-" + random.nextInt(10));
            event.put("ville", ville);
            event.put("timestamp", Instant.now().toString());

            if (!type.equals("OUVERTURE")) {
                double montant = 5 + random.nextInt(500);
                event.put("montant", montant);
            }

            event.put("produits", Arrays.asList("pain", "lait"));

            String json = mapper.writeValueAsString(event);

            ProducerRecord<String, String> record =
                    new ProducerRecord<>("pos-events", ville, json);

            producer.send(record);

            Thread.sleep(100 + random.nextInt(400));
        }
    }
}
