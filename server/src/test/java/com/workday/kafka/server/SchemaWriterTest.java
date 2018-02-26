package com.workday.kafka.server;

import com.workday.kafka.kafky.User;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.util.Properties;
import java.util.stream.IntStream;

/**
 * Created by drewwilson on 23/02/2018.
 */
public class SchemaWriterTest {

        private static Producer<Long, User> createProducer() {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    LongSerializer.class.getName());

            // Configure the KafkaAvroSerializer.
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    KafkaAvroSerializer.class.getName());

            // Schema Registry location.
            props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                    "http://localhost:8081");

            return new KafkaProducer<>(props);
        }

        private final static String TOPIC = "new-employees";

        public static void main(String... args) {

            Producer<Long, User> producer = createProducer();

            IntStream.range(1, 100).forEach(index->{
                User user = User.newBuilder().setId(index).setAmount(20.0*index).setTimestamp(7L*index).build();
                producer.send(new ProducerRecord<>(TOPIC, 1L * index, user));

            });

            producer.flush();
            producer.close();
        }

}
