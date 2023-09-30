package app.nourtabib.financialdatastreamingapp.streams;


import app.nourtabib.financialdatastreamingapp.avros.AccountActivityAggregate;
import app.nourtabib.financialdatastreamingapp.avros.AccountIncomeContext;
import app.nourtabib.financialdatastreamingapp.avros.AccountOutcomeContext;
import app.nourtabib.financialdatastreamingapp.streams.utils.TimeStampExtractors;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class AccountActivityAggregationStageStream {



    // AGGREGATION STAGES :
    // (RAW_TRANSACTIONS) -> WINDOWING BY 10SECS (COUNT,TOTAL) -> WINDOWING BY 1HOURS  -> (COUNT,TOTAL,1MIN_WINDOW) -> WINDOWING BY 1DAY-> (COUNT,TOTAL,1DAY_WINDOW)
    // THIS APPROACH INTRODUCES A SOME LATENCY IN EACH STAGE
    // BECAUSE UPSTREAM EMIT THE WINDOW'S AGGREGATIONS RESULTS AFTER THE (5SEC and 1MIN) GRACE PERIODS
    // BUT IT GUARANTEES ORDER IN THE DOWNSTREAM
    @Autowired
    public void buildTopologyFirstApproach(StreamsBuilder streamsBuilder){

        Map<String,String> schemaRegistryConfig = Map.of("schema.registry.url", "http://localhost:8081");

        Serde<AccountIncomeContext> accountIncomeContextSerde =  new SpecificAvroSerde<>();
        Serde<AccountOutcomeContext> accountOutcomeContextSerde=  new SpecificAvroSerde<>();
        Serde<AccountActivityAggregate> accountActivityAggregationSerde=  new SpecificAvroSerde<>();
        Serde<List<AccountActivityAggregate>> accountActivityAggregationListSerde = new Serdes.ListSerde(ArrayList.class,accountActivityAggregationSerde);
        Serde<List<AccountIncomeContext>> accountIncomeAggregationListSerde = new Serdes.ListSerde(ArrayList.class,accountIncomeContextSerde);
        Serde<List<Double>> longListSerde = new Serdes.ListSerde(ArrayList.class,Serdes.Double());


        accountActivityAggregationSerde.configure(
                schemaRegistryConfig,
                false);
        accountIncomeContextSerde.configure(
                schemaRegistryConfig,
                false);
        accountOutcomeContextSerde.configure(
                schemaRegistryConfig
                ,false);
//        accountActivityAggregationListSerde.configure(
//                schemaRegistryConfig,
//                false);

        Duration oneSecond = Duration.ofSeconds(1L);
        Duration twoSeconds = Duration.ofSeconds(2L);
        Duration tenSeconds = Duration.ofSeconds(10L);
        Duration oneMinute = Duration.ofMinutes(1L);
        Duration oneHours = Duration.ofHours(1L);
        Duration oneDay = Duration.ofDays(1L);
        Duration oneWeek = Duration.ofDays(7L);
        Duration oneMonth = Duration.ofDays(30L);
        Duration threeMonths = Duration.ofDays(30L);


        TimeWindows tenSecondsWindow = TimeWindows.ofSizeAndGrace(tenSeconds,twoSeconds);
        TimeWindows oneMinuteWindow = TimeWindows.ofSizeAndGrace(oneMinute,twoSeconds);
        TimeWindows oneHourWindow = TimeWindows.ofSizeAndGrace(oneHours,twoSeconds);
        TimeWindows oneDayWindow = TimeWindows.ofSizeAndGrace(oneDay,twoSeconds);
        TimeWindows oneWeekWindow = TimeWindows.ofSizeAndGrace(oneWeek,twoSeconds);
        TimeWindows oneMonthsWindow = TimeWindows.ofSizeAndGrace(oneMonth,twoSeconds);
        TimeWindows threeMonthsWindow = TimeWindows.ofSizeAndGrace(threeMonths,twoSeconds);


        Consumed<String,AccountIncomeContext> accountIncContextConsumedWith = Consumed
                .with(Serdes.String(),accountIncomeContextSerde)
                .withTimestampExtractor(new TimeStampExtractors.AccountIncomeContextTimeStampExtractor());
        Consumed<String,AccountOutcomeContext> accountOutContextConsumedWith = Consumed
                .with(Serdes.String(),accountOutcomeContextSerde)
                .withTimestampExtractor(new TimeStampExtractors.AccountOutcomeContextTimeStampExtractor());
        Produced<?,?> accountActivityDailyAggregateProducedWith = Produced
                .with(Serdes.String(),accountIncomeAggregationListSerde);

        KStream<String, AccountIncomeContext> accountIncomeContextKStream = streamsBuilder.stream("account-income-context",accountIncContextConsumedWith);
        KStream<String, AccountOutcomeContext> accountOutcomeContextKStream = streamsBuilder.stream("account-outcome-context",accountOutContextConsumedWith);


       // KTable<Windowed<String>,AccountActivityAggregation> tenSecondIncomeActivityAggregation =
                 accountIncomeContextKStream
                .groupByKey()
                .windowedBy(tenSecondsWindow.advanceBy(tenSeconds))
                .aggregate(
                        ()-> new AccountActivityAggregate(0.0,0, Instant.ofEpochMilli(0L)),
                        (key,newRecord,aggValue)-> {
                            aggValue.setTotal(aggValue.getTotal() + newRecord.getAmount());
                            aggValue.setCount(aggValue.getCount() + 1);
                            if(newRecord.getTimestamp().isAfter(aggValue.getTimestamp())){
                                aggValue.setTimestamp(newRecord.getTimestamp());
                            }
                            return aggValue;
                        },
                        Materialized.<String, AccountActivityAggregate, WindowStore<Bytes,byte[]>>
                                        as("INCOME-ACTIVITY-1DAY-AGGREGATES-VALUES")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(accountActivityAggregationSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()       // KTable<Windowed<String>,AccountActivityAggregation> oneHourIncomeActivityAggregation = tenSecondIncomeActivityAggregation
                .groupBy((key,value) -> key.key())
                .windowedBy(oneHourWindow.advanceBy(oneHours))
                .aggregate(
                        ()-> new AccountActivityAggregate(0.0,0,Instant.ofEpochMilli(0L)),
                        (key,newResult,aggValue)->{
                            aggValue.setTotal(aggValue.getTotal() + newResult.getTotal());
                            aggValue.setCount(aggValue.getCount() + newResult.getCount());
                            if(newResult.getTimestamp().isAfter(aggValue.getTimestamp())){
                                aggValue.setTimestamp(newResult.getTimestamp());
                            }
                            return aggValue;
                        },
                        Materialized.<String, AccountActivityAggregate,WindowStore<Bytes,byte[]>>
                                        as("INCOME-ACTIVITY-1HOUR-AGGREGATES-VALUES")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(accountActivityAggregationSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())).toStream()//        KTable<Windowed<String>,AccountActivityAggregation> oneDayIncomeActivityAggregation = oneHourIncomeActivityAggregation
                .groupBy((key,value)-> key.key())
                .windowedBy(oneDayWindow.advanceBy(oneDay))
                .aggregate(
                        ()-> new AccountActivityAggregate(0.0,0,Instant.ofEpochMilli(0L)),
                        (key,newResult,aggValue)->{
                            aggValue.setTotal(aggValue.getTotal() + newResult.getTotal());
                            aggValue.setCount(aggValue.getCount() + newResult.getCount());
                            if(newResult.getTimestamp().isAfter(aggValue.getTimestamp())){
                                aggValue.setTimestamp(newResult.getTimestamp());
                            }
                            return aggValue;
                        },
                        Materialized.<String, AccountActivityAggregate,WindowStore<Bytes,byte[]>>as("INCOME-ACTIVITY-1DAY-AGGREGATES-VALUES")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(accountActivityAggregationSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()//oneDayIncomeActivityAggregation
                .selectKey((key,value)->key.key())
                .groupByKey()
                .windowedBy(oneMonthsWindow.advanceBy(oneMonth))
                .aggregate(
                        ArrayList<AccountActivityAggregate>::new,
                        (key,newResult,aggList) -> {
                            aggList.add(newResult);
                            return aggList;
                        },
                        Materialized.<String,List<AccountActivityAggregate>,WindowStore<Bytes,byte[]>>as("INCOME-ACTIVITY-1MONTH-AGGREGATES-HISTORY-DAILY-VALUES")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(accountActivityAggregationListSerde)
                )
                .toStream()
                .to("account-income-activity-month-daily-aggregates");
    }
    public void testCase(StreamsBuilder streamsBuilder){
        // WORKING TEST CASE
        // KEPT FOR REVISION CASES
//        KTable<Windowed<String>,List<Double>> tenSecondActivityAggregation1 = accountIncomeContextKStream
//                .peek((key,value) -> {
//                    System.out.println(value.getTimestamp().toEpochMilli());
//                })
//                .groupByKey() // GROUPING BY ACCOUNT ID
//                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10L),Duration.ofSeconds(2L)).advanceBy(Duration.ofSeconds(10L)))// WINDOWING BY 10 SECONDS
//                .aggregate(
//                        ArrayList<Double>::new,
//                        (key,newRecord,aggValue)-> {
//                            aggValue.add(newRecord.getAmount());
//                            return aggValue;
//                        },
//                        Materialized.<String,List<Double>, WindowStore<Bytes,byte[]>>as("GROUPED-KTTABLE-EVENTS-STORE-4")
//                                .withKeySerde(Serdes.String())
//                                .withValueSerde(new Serdes.ListSerde<>(ArrayList.class,Serdes.Double()))
//                )
//                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));
//        tenSecondActivityAggregation1.toStream().foreach((key,value)->{
//            BufferedWriter writer;
//            try {
//                writer = new BufferedWriter(new FileWriter("C:\\Users\\Nour.Tabib\\Desktop\\financial-data-streaming-app\\src\\main\\output.txt", true));
//
//            } catch (IOException e) {
//                e.printStackTrace();
//                throw new RuntimeException(e);
//            }
////            System.out.println("["+key.window().start()+":"+key.window().end()+"]"+ value.toString());
//            try {
//                writer.write("["+key.window().start()+":"+key.window().end()+"]"+ value.toString());
//            } catch (IOException e) {
//                e.printStackTrace();
//                throw new RuntimeException(e);
//            }
//            try {
//                writer.newLine();
//                writer.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//                throw new RuntimeException(e);
//            }
//
//            // You can repeat the above two lines as many times as needed for additional lines
//
//            // Close the writer to save changes and release resources
//
//        });
    }
}
