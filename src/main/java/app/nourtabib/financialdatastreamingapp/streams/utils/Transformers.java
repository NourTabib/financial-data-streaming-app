package app.nourtabib.financialdatastreamingapp.streams.utils;

import app.nourtabib.financialdatastreamingapp.avros.AccountActivityAggregate;
import com.github.psambit9791.jdsp.transform.ShortTimeFourier;
import com.github.psambit9791.jdsp.transform._Fourier;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Transformers {
    public Transformers() {
    }

    @FunctionalInterface
    public interface SignalExtractor<T> {
        public double[] apply(List<T> aggregationList,int paddedSize,String granularity);
    }
    @FunctionalInterface
    public interface StfTransformer {
        public double[] apply(double[] signals,int windowSize,int overlap);
    }
    public static final SignalExtractor<AccountActivityAggregate> extractTotalsFromWindowActivity = (aggregationList, paddedSize, granularity) -> {
        if(aggregationList.size() < 1){
            return new double[paddedSize];
        }
        else{
            List<Double> paddedAggregList = new ArrayList<>(paddedSize) ;
            for (int i = 0; i < paddedSize; i++) {paddedAggregList.add(0.0);}
            for(AccountActivityAggregate activity : aggregationList){
                int timeUnit;
                if( granularity.equals("hourly")){ timeUnit = activity.getTimestamp().atZone(ZoneId.of("UTC")).getHour(); }
                else if( granularity.equals("daily") ){ timeUnit = activity.getTimestamp().atZone(ZoneId.of("UTC")).getDayOfMonth() ;}
                else { timeUnit = -1 ;}
                if (timeUnit >= 0 && timeUnit < paddedSize) {paddedAggregList.set(timeUnit, activity.getTotal());}
            }
            return paddedAggregList
                    .stream()
                    .mapToDouble(Double::doubleValue)
                    .toArray();
        }
    };
    public static final StfTransformer stfTransform = (signals,windowSize,overlap) -> {
        ShortTimeFourier transformer = new ShortTimeFourier(signals,windowSize,overlap);
        transformer.transform();
        transformer.getTimeAxis();
        _Fourier[] dfts = transformer.getOutput();
        System.out.println(Arrays.toString(transformer.getTimeAxis()));
        Arrays.stream(dfts).forEach((value)-> {
            System.out.println("****START****");
            System.out.println(Arrays.toString(value.getMagnitude(false)));
            System.out.println(Arrays.toString(value.getFFTFreq(31,false)));
            System.out.println(Arrays.toString(value.getMagnitude(false)));
            System.out.println("****END****");
        });
        return transformer.getFrequencyAxis(false);
    };
}
