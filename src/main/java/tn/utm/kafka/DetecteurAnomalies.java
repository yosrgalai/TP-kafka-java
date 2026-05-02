package tn.utm.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.*;

public class DetecteurAnomalies {

    public static void main(String[] args) throws Exception {

        // ===== CONSUMER CONFIG =====
        Properties cprops = new Properties();
        cprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        cprops.put(ConsumerConfig.GROUP_ID_CONFIG, "alerte-1");
        cprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cprops);
        consumer.subscribe(Collections.singletonList("pos-events"));

        // ===== PRODUCER CONFIG =====
        Properties pprops = new Properties();
        pprops.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        pprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(pprops);

        ObjectMapper mapper = new ObjectMapper();

        System.out.println("🚨 Detecteur d'anomalies démarré...");

        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<String, String> record : records) {

                Map<String, Object> event = mapper.readValue(record.value(), Map.class);

                String type = (String) event.get("type");

                if ("RETOUR".equals(type) && event.get("montant") != null) {

                    double montant = ((Number) event.get("montant")).doubleValue();

                    if (montant > 200) {

                        // 🚨 Anomalie détectée
                        System.out.println("⚠️ RETOUR ANORMAL détecté : " + record.value());

                        // envoi vers topic alertes
                        ProducerRecord<String, String> alert =
                                new ProducerRecord<>("alertes-retours", record.key(), record.value());

                        producer.send(alert);
                    }
                }
            }

            // commit après traitement
            if (!records.isEmpty()) {
                consumer.commitSync();
            }
        }
    }
}

