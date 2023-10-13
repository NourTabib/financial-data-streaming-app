package app.nourtabib.financialdatastreamingapp.streams;

import app.nourtabib.financialdatastreamingapp.avros.AccountIncomeContext;
import app.nourtabib.financialdatastreamingapp.avros.AccountOutcomeContext;
import app.nourtabib.financialdatastreamingapp.avros.RawTransactionAvro;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class ContextDispatchingStream {


    @Value("${spring.kafka.topic.raw-transactions-events}")
    public static String rawTransactionsEventsTopic;

    @Value("${spring.kafka.topic.account-income-context}")
    public static String accountIncomeContextTopic;

    @Value("${spring.kafka.topic.account-outcome-context}")
    public static String accountOutcomeContextTopic;


    KafkaTemplate<String, RawTransactionAvro> kafkaTemplate;
    @Autowired
    public ContextDispatchingStream(KafkaTemplate<String, RawTransactionAvro> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }
    @Autowired
    public void buildTopology(StreamsBuilder streamsBuilder){
        Map<String,String> schemaRegistryConfig = Map.of("schema.registry.url", "http://localhost:8081");


        Serde<RawTransactionAvro> rawTransactionAvroSerde = new SpecificAvroSerde<>();
        Serde<AccountIncomeContext> accountIncomeContextSerde =  new SpecificAvroSerde<>();
        Serde<AccountOutcomeContext> accountOutcomeContextSerde=  new SpecificAvroSerde<>();

        rawTransactionAvroSerde.configure(
                schemaRegistryConfig,
                false);
        accountIncomeContextSerde.configure(
                schemaRegistryConfig
                , false
        );
        accountOutcomeContextSerde.configure(
                schemaRegistryConfig,
                false);


        Consumed<String, RawTransactionAvro> consumedWith = Consumed.with(Serdes.String(), rawTransactionAvroSerde);
        Produced<String, AccountIncomeContext> accountIncomeContextProduced = Produced.with(Serdes.String(), accountIncomeContextSerde);
        Produced<String, AccountOutcomeContext> accountOutcomeContextProduced = Produced.with(Serdes.String(), accountOutcomeContextSerde);


        KStream<String, RawTransactionAvro> rawTransactionAvroKStream = streamsBuilder
                .stream("raw-transaction-events",consumedWith);


        KStream<String, AccountIncomeContext> incomeContextKStream = rawTransactionAvroKStream
                .map((transactionId, rawTransaction) -> {
                    AccountIncomeContext accountIncomeContext = AccountIncomeContext.newBuilder()
                            .setTransactionId(rawTransaction.getTransactionId())
                            .setTimestamp(rawTransaction.getTimestamp())
                            .setType(rawTransaction.getType())
                            .setNameOrig(rawTransaction.getNameOrig())
                            .setAmount(rawTransaction.getAmount())
                            .build();
                    return new KeyValue<String, AccountIncomeContext>(rawTransaction.getNameDest().toString(), accountIncomeContext);
                });

            incomeContextKStream.to("account-income-activity",accountIncomeContextProduced);

        KStream<String, AccountOutcomeContext> outcomeContextKStream = rawTransactionAvroKStream
                .map((transactionId, rawTransaction) -> {
                    AccountOutcomeContext accountOutcomeContext = AccountOutcomeContext.newBuilder()
                            .setTransactionId(rawTransaction.getTransactionId())
                            .setTimestamp(rawTransaction.getTimestamp())
                            .setType(rawTransaction.getType())
                            .setAmount(rawTransaction.getAmount())
                            .setNameDest(rawTransaction.getNameDest())
                            .build();
                    return new KeyValue<String, AccountOutcomeContext>(rawTransaction.getNameOrig().toString(), accountOutcomeContext);
                });
        outcomeContextKStream.to("account-outcome-activity",accountOutcomeContextProduced);
    }
}
