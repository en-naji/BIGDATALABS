package edu.supmti.kafka; // <-- CETTE LIGNE EST OBLIGATOIRE !

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class EventProducer {
    public static void main(String[] args) throws Exception{

        // Verifier que le topic est fourni comme arg
        if(args.length == 0){
            System.out.println("Entrer le nom du topic");
            return;
        }

        String topicName = args[0].toString(); // lire le topicName fourni comme param

        // J'ai corrigé la ligne ci-dessous (il manquait une barre oblique /)
        Properties props = new Properties(); // acceder aux configurations du producteur

        // Important : pour que ça marche DANS Docker, il faut utiliser le nom du conteneur Kafka, pas "localhost"
        props.put("bootstrap.servers", "kafka:9092"); // <-- CORRECTION ICI POUR DOCKER

        // Definir un acquittement pour les requetes du producteur
        props.put("acks", "all");
        // Si la requete echoue, le producteur peut reessayer automatiquement
        props.put("retries", 0);
        // Specifier la taille du buffer size dans la config
        props.put("batch.size", 16384);
        // controle l espace total de mem dispo au producteur pour le buffering
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String,String>(props);

        for(int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), "Message numero " + Integer.toString(i)));
        }

        System.out.println("10 messages envoyés avec succès au topic " + topicName);
        producer.close();
    }
}