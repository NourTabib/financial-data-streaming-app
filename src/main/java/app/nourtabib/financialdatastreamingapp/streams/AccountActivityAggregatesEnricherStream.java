package app.nourtabib.financialdatastreamingapp.streams;

import app.nourtabib.financialdatastreamingapp.avros.AccountActivityAggregate;
import app.nourtabib.financialdatastreamingapp.avros.AccountIncomeContext;
import app.nourtabib.financialdatastreamingapp.avros.AccountOutcomeContext;
import app.nourtabib.financialdatastreamingapp.streams.utils.TimeStampExtractors;
import app.nourtabib.financialdatastreamingapp.streams.utils.Transformers;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Component
public class AccountActivityAggregatesEnricherStream {
    @Autowired
    public void buildTopology(StreamsBuilder streamsBuilder){

        Serde<AccountIncomeContext> accountIncomeContextSerde =  new SpecificAvroSerde<>();
        Serde<AccountOutcomeContext> accountOutcomeContextSerde=  new SpecificAvroSerde<>();
        Serde<AccountActivityAggregate> accountActivityAggregationSerde=  new SpecificAvroSerde<>();
        Serde<List<AccountActivityAggregate>> accountActivityAggregationListSerde = new Serdes.ListSerde(ArrayList.class,accountActivityAggregationSerde);
        Serde<List<AccountIncomeContext>> accountIncomeAggregationListSerde = new Serdes.ListSerde(ArrayList.class,accountIncomeContextSerde);
        Serde<List<Double>> doubleListSerde = new Serdes.ListSerde(ArrayList.class,Serdes.Double());

        Consumed<String, List<AccountActivityAggregate>> consumedWith = Consumed
                .with(Serdes.String(), accountActivityAggregationListSerde)
                .withKeySerde(Serdes.String())
                .withValueSerde(accountActivityAggregationListSerde)
                .withTimestampExtractor(new TimeStampExtractors.AccountActivityAggregateTimestampExtractor());

        Duration sixMonths = Duration.ofDays(6L*30L);
        Duration fiveMinutes = Duration.ofMinutes(5L);
        Duration oneDay = Duration.ofDays(1L);

        TimeWindows oneMonthWindow = TimeWindows.ofSizeAndGrace(sixMonths , oneDay);

        KStream<String,List<AccountActivityAggregate>> accountIncomeActivityOneMonthDailyAggregStreams = streamsBuilder
                .stream("account-income-activity-month-daily-aggregates",consumedWith);
        KStream<String,List<AccountActivityAggregate>> accountOutcomeActivityOneMonthDailyAggregStreams = streamsBuilder
                .stream("account-outcome-activity-month-daily-aggregates");


        accountIncomeActivityOneMonthDailyAggregStreams
                .mapValues((key,value) -> Transformers.extractTotalsFromWindowActivity.apply(value,30,"daily"))
                .mapValues((key,value) -> Transformers.stfTransform.apply(value,30,29))
                .groupByKey()
                .windowedBy(oneMonthWindow.advanceBy(oneDay))

        ;
    }
}
