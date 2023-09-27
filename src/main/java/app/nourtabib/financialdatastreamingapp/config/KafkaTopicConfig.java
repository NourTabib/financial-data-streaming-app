package app.nourtabib.financialdatastreamingapp.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    public static String bootstrapServers;

    @Value("${spring.kafka.schema-registry.url}")
    public static String schemaRegistryUrl;

    @Value("${spring.kafka.topic.raw-transactions-events}")
    public static String rawTransactionsEventsTopic;

    @Value("${spring.kafka.topic.account-income-context}")
    public static String accountIncomeContextTopic;

    @Value("${spring.kafka.topic.account-outcome-context}")
    public static String accountOutcomeContextTopic;
//    public String accountOutcomeContextTransfert;
//    @Value("${spring.kafka.topic.account-outcome-context-debit}")
//    public String accountOutcomeContextDebit;
//    @Value("${spring.kafka.topic.account-outcome-context-payment}")
//    public String accountOutcomeContextPayment;

    @Value("${spring.kafka.stream.num-streams-threads}")
    public static int numStreamsThreads;

    @Value("${spring.kafka.streams.applications.context-dispatching-app.id}")
    public static String contextDispatchingAppId;

    @Value("${spring.kafka.streams.applications.preprocessing-stage-one.id}")
    public static String preprocessingStageOneAppId;

    @Value("${spring.kafka.topic.transaction.state.log.replication.factor}")
    public static int transactionStateLogReplicationFactor;

    @Bean
    public KafkaAdmin kafkaAdmin(){
        Map<String,Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"http://localhost:29092");
        return new KafkaAdmin(configs);
    }
    @Bean
    public NewTopic rawTransactionsEventsTopic() {
        return new NewTopic("raw-transaction-events", 1, (short) 1);
    }

    @Bean
    public NewTopic accountIncomeContextTopic() {
        return new NewTopic("account-income-context", 1, (short) 1);
    }

    @Bean
    public NewTopic accountOutcomeContextTopic() {
        return new NewTopic("account-outcome-context", 1, (short) 1);
    }
}
