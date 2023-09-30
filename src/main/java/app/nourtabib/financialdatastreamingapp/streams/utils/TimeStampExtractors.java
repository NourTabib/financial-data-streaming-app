package app.nourtabib.financialdatastreamingapp.streams.utils;

import app.nourtabib.financialdatastreamingapp.avros.AccountActivityAggregate;
import app.nourtabib.financialdatastreamingapp.avros.AccountIncomeContext;
import app.nourtabib.financialdatastreamingapp.avros.AccountOutcomeContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeStampExtractors {
    private static final Logger logger = LoggerFactory.getLogger(TimestampExtractor.class);


    public static class AccountActivityAggregateTimestampExtractor implements  TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
            try {
                long time = ((AccountActivityAggregate) consumerRecord.value()).getTimestamp().toEpochMilli();
                return time;
            }catch (Exception e) {
                e.printStackTrace();
                return -1;
            }

        }
    }
    public static class AccountIncomeContextTimeStampExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
            try{
                long time = ((AccountIncomeContext) consumerRecord.value()).getTimestamp().minusNanos(10).toEpochMilli();
                return time;
            }catch (Exception e){
                e.printStackTrace();
                return -1L;
            }
        }
    }
    public static class AccountOutcomeContextTimeStampExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
            try{
                long time = ((AccountOutcomeContext) consumerRecord.value()).getTimestamp().toEpochMilli();
                return time;
            }catch (Exception e){
                e.printStackTrace();
                return -1L;
            }
        }
    }


}
