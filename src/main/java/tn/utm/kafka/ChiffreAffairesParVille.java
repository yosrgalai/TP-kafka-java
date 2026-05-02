package tn.utm.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.*;

public class ChiffreAffairesParVille {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ca-1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("pos-events"));

        ObjectMapper mapper = new ObjectMapper();

        // Map ville → chiffre d'affaires
        Map<String, Double> caParVille = new HashMap<>();

        long lastPrint = System.currentTimeMillis();

        System.out.println("📊 Consumer CA démarré...");

        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<String, String> record : records) {

                Map<String, Object> event = mapper.readValue(record.value(), Map.class);

                String type = (String) event.get("type");
                String ville = (String) event.get("ville");

                double montant = 0.0;
                if (event.get("montant") != null) {
                    montant = ((Number) event.get("montant")).doubleValue();
                }

                caParVille.putIfAbsent(ville, 0.0);

                if ("VENTE".equals(type)) {
                    caParVille.put(ville, caParVille.get(ville) + montant);
                } else if ("RETOUR".equals(type)) {
                    caParVille.put(ville, caParVille.get(ville) - montant);
                }
            }

            // commit après traitement
            if (!records.isEmpty()) {
                consumer.commitSync();
            }

            // affichage toutes les 5 secondes
            if (System.currentTimeMillis() - lastPrint > 5000) {
                System.out.println("====== CA PAR VILLE ======");
                for (Map.Entry<String, Double> entry : caParVille.entrySet()) {
                    System.out.printf("%s : %.2f DT%n", entry.getKey(), entry.getValue());
                }
                System.out.println("==========================");
                lastPrint = System.currentTimeMillis();
            }
        }
    }
}
