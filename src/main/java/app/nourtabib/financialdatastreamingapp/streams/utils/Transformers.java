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
    @FunctionalInterface
    public interface StftMeanComputation {
        public Object apply(ShortTimeFourierResult shortTimeFourierResult);
    }
    @FunctionalInterface
    public interface StftVarianceComputation {
        public Object apply(ShortTimeFourierResult shortTimeFourierResult,Double mean);
    }
    @FunctionalInterface
    public interface StftSkewnessComputation {
        public Object apply(ShortTimeFourierResult shortTimeFourierResult,Double mean,Double variance);
    }
    @FunctionalInterface
    public interface StftKurtosisComputation {
        public Object apply(ShortTimeFourierResult shortTimeFourierResult,Double mean,Double variance);
    }
    @FunctionalInterface
    public interface StftTimeSparsityComputation {
        public Object apply(ShortTimeFourierResult shortTimeFourierResult,Double mean,Double variance);
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

        // Init Result Object
        ShortTimeFourierResult result = new ShortTimeFourierResult();
        result.setOutputFFTFreqs(new ArrayList<>());
        result.setOutputMagnitudes(new ArrayList<>());

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
    public static final StftMeanComputation computeStftMean = (shortTimeFourierResult) -> {
        double T = (double) shortTimeFourierResult.getTimeAxis().size();
        double F = (double) shortTimeFourierResult.getFrequencyAxis().size();
        double scalingFactor = 1 / (T*F);
        Double sum = shortTimeFourierResult
                .getOutputMagnitudes()
                .stream()
                .map(magnitude -> {
                    return magnitude.stream().mapToDouble(Double::doubleValue).sum();
                })
                .mapToDouble(Double::doubleValue)
                .sum();
        return scalingFactor * sum;
    };
    public static final StftVarianceComputation computeStftVariance = (shortTimeFourierResult,mean) -> {
        double T = (double) shortTimeFourierResult.getTimeAxis().size();
        double F = (double) shortTimeFourierResult.getFrequencyAxis().size();
        double scalingFactor = 1 / (T*F);
        Double sum = shortTimeFourierResult
                .getOutputMagnitudes()
                .stream()
                .map(magnitudes -> {
                    return magnitudes
                            .stream()
                            .map(magnitude -> magnitude - mean)
                            .map(normValue -> Math.pow(normValue,2.0))
                            .mapToDouble(Double::doubleValue)
                            .sum();
                })
                .mapToDouble(Double::doubleValue)
                .sum();
        return Math.sqrt(scalingFactor * sum);
    };
    public static final StftSkewnessComputation computeStftSkewness = (shortTimeFourierResult,mean,variance) -> {
        double T = (double) shortTimeFourierResult.getTimeAxis().size();
        double F = (double) shortTimeFourierResult.getFrequencyAxis().size();
        double scalingFactor = 1 / (T*F);
        Double sum = shortTimeFourierResult
                .getOutputMagnitudes()
                .stream()
                .map(magnitudes -> {
                    return magnitudes
                            .stream()
                            .map(magnitude -> magnitude - mean)
                            .map(normValue -> Math.pow(normValue,3.0))
                            .map(numerator -> numerator / (Math.pow(variance,3.0)))
                            .mapToDouble(Double::doubleValue)
                            .sum();
                })
                .mapToDouble(Double::doubleValue)
                .sum();
        return scalingFactor * sum;
    };
    public static final StftKurtosisComputation computeStftKurtosis = (shortTimeFourierResult,mean,variance) -> {
        double T = (double) shortTimeFourierResult.getTimeAxis().size();
        double F = (double) shortTimeFourierResult.getFrequencyAxis().size();
        double scalingFactor = 1 / (T*F);
        Double sum = shortTimeFourierResult
                .getOutputMagnitudes()
                .stream()
                .map(magnitudes -> {
                    return magnitudes
                            .stream()
                            .map(magnitude -> magnitude - mean)
                            .map(normValue -> Math.pow(normValue,4.0))
                            .map(numerator -> numerator / (Math.pow(variance,4.0)))
                            .mapToDouble(Double::doubleValue)
                            .sum();
                })
                .mapToDouble(Double::doubleValue)
                .sum();
        return scalingFactor * sum;
    };
}

