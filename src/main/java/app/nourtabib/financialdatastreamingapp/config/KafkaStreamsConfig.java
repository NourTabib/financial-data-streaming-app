package app.nourtabib.financialdatastreamingapp.config;


import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.kafka.streams.StreamsConfig.*;

@Configuration
@EnableKafkaStreams
@EnableKafka
public class KafkaStreamsConfig {
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration kStreamsConfig(){
        Map<String,Object> props = new HashMap<>();
        props.put(NUM_STREAM_THREADS_CONFIG,1);
        props.put(APPLICATION_ID_CONFIG, "appp");
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put("transaction.state.log.replication.factor", 1);
        props.put("schema.registry.url", "http://localhost:8081");
        props.put(STATE_DIR_CONFIG,"./src/main/kafka-state-dir");
        return new KafkaStreamsConfiguration(props);
    }
}
