package de.lwerner.flink.percentiles.functions.join;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Function which calculates the weighted median by given medians and weights.
 *
 * @author Lukas Werner
 */
public class CalculateWeightedMedianGroupReduceFunction implements GroupReduceFunction<Tuple2<Double, Double>, Tuple1<Double>> {

    @Override
    public void reduce(Iterable<Tuple2<Double, Double>> iterable, Collector<Tuple1<Double>> collector) throws Exception {
        List<Double> medians = new ArrayList<>();
        List<Double> weights = new ArrayList<>();
        for (Tuple2<Double, Double> t: iterable) {
            medians.add(t.f0);
            weights.add(t.f1);
        }

        for (double mk: medians) {
            double sumLower = 0;
            double sumHigher = 0;
            for (int j = 0; j < medians.size(); j++) {
                double mi = medians.get(j);
                double wi = weights.get(j);

                if (mi < mk) {
                    sumLower += wi;
                } else if (mi > mk) {
                    sumHigher += wi;
                }
            }

            if (sumLower <= 0.5 && sumHigher <= 0.5) {
                collector.collect(new Tuple1<>(mk));
                break;
            }
        }
    }

}