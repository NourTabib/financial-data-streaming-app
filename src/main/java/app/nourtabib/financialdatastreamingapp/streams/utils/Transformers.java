package app.nourtabib.financialdatastreamingapp.streams.utils;

import app.nourtabib.financialdatastreamingapp.avros.AccountActivityAggregate;
import app.nourtabib.financialdatastreamingapp.avros.ShortTimeFourierResult;
import com.github.psambit9791.jdsp.transform.ShortTimeFourier;
import com.github.psambit9791.jdsp.transform._Fourier;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Transformers {
    public Transformers() {
    }

    @FunctionalInterface
    public interface SignalExtractor<T> {
        public double[] apply(List<T> aggregationList,int paddedSize,String granularity);
    }
    @FunctionalInterface
    public interface StfTransformer {
        public ShortTimeFourierResult apply(double[] signals,int windowSize,int overlap);
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
        _Fourier[] dfts = transformer.getOutput();
        ShortTimeFourierResult result = new ShortTimeFourierResult();

        // Setting the Time Axis
        result.setTimeAxis(
                Arrays.stream(transformer.getTimeAxis())
                        .boxed()
                        .collect(Collectors.toList())
        );
        // Setting The Frequency Axis
        result.setFrequencyAxis(
                Arrays.stream(transformer.getFrequencyAxis(false))
                        .boxed()
                        .collect(Collectors.toList())
        );
        // Iterating Over the Fourier Transformation Output
        Arrays.stream(dfts).forEach(value -> {
            result.getOutputFFTFreqs().add(
                    Arrays.stream(value.getFFTFreq(4,false)).boxed().collect(Collectors.toList())
            );
            result.getOutputMagnitudes().add(
                    Arrays.stream(value.getMagnitude(false)).boxed().collect(Collectors.toList())
            );
        });
        return result;
    };
}

