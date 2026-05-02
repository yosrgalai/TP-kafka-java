# 📦 TP Kafka — Traitement de données en temps réel
## 🎯 Objectif

Découvrir et maîtriser les concepts clés de Apache Kafka :


Topics & partitions

Producers / Consumers

Consumer groups

Gestion des offsets

Sérialisation JSON

Pipeline temps réel

## ⚙️ Installation & Démarrage
- Télécharger Kafka

cd ~

wget https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz

tar -xzf kafka_2.13-3.7.0.tgz

mv kafka_2.13-3.7.0 kafka

- Variables d’environnement

export KAFKA_HOME="$HOME/kafka"

export PATH="$KAFKA_HOME/bin:$PATH"

- Démarrage Kafka

kafka-server-start.sh -daemon ~/kafka-data/server.properties

## 📚 Manipulation de base
- Créer un topic

kafka-topics.sh --create \
-topic ventes \
-bootstrap-server localhost:9092 \
-partitions 3 \
-replication-factor 1

- Lister les topics

kafka-topics.sh --list --bootstrap-server localhost:9092

- Producer (console)

kafka-console-producer.sh \
--bootstrap-server localhost:9092 \
--topic ventes

- Consumer (console)

kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic ventes \
--from-beginning

## 🔑 Concepts essentiels
Partition → permet le parallélisme
Clé → garantit que les messages similaires vont dans la même partition
Consumer group → répartit la charge
Offset → position du message dans la partition

## 💻 Partie Java
- Producer

Envoi de messages avec clé
Idempotence activée (pas de doublons)
Callback avec partition + offset

- Consumer

Lecture en boucle
Commit manuel des offsets
Traitement fiable (at least once)

## 🧾 Sérialisation JSON
Utilisation de Jackson

Conversion objet → JSON → Kafka

Désérialisation côté consumer


# 🏗️ Mini-Projet : Pipeline temps réel
- Architecture

Simulateurs (Producers)

        ↓
    pos-events
    
        ↓
┌───────────────┬───────────────┐
               ↓
 
CA Consumer   Anomaly Detector


- SimulateurCaisse

Génère événements (VENTE, RETOUR, OUVERTURE)

Clé = ville

Idempotence activée

- ChiffreAffairesParVille

Calcule CA par ville

(VENTE - RETOUR)

Affichage toutes les 5 secondes

Commit manuel

- DetecteurAnomalies

Groupe différent (alerte-1)

Détecte RETOUR > 200 DT

Envoie vers topic alertes-retours


## 🧪 Tests réalisés

- Scalabilité

Plusieurs producers → montée en charge

- Parallélisme

Plusieurs consumers → rebalance automatique

- Tolérance aux pannes

Crash d’un consumer → reprise par un autre

- Monitoring

Observation du LAG avec kafka-consumer-groups


## 📊 Points clés

1 partition = 1 consumer maximum

Plus de partitions → plus de parallélisme

LAG ↑ si consommation lente

Idempotence → évite les doublons

## ✅ Conclusion

Ce TP permet de construire un pipeline temps réel complet et fiable avec Kafka :

ingestion des données

traitement en continu

détection d’anomalies

distribution des résultats
