package de.lwerner.flink.percentiles.functions.join;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

/**
 * Function which maps each double value to a tuple (x, y, z) which says, if the element is less (1, 0, 0), equal
 * (0, 1, 0) or greater (0, 0, 1) than the weighted median.
 *
 * @author Lukas Werner
 */
public class CalculateLessEqualAndGreaterMapFunction extends RichMapFunction<Tuple3<Double, Integer, Integer>, Tuple5<Integer, Integer, Integer, Integer, Integer>> {

    /**
     * The weighted median
     */
    private double weightedMedian;

    @Override
    public void open(Configuration parameters) throws Exception {
        Collection<Tuple1<Double>> weightedMedian = getRuntimeContext().getBroadcastVariable("weightedMedian");

        for (Tuple1<Double> t: weightedMedian) {
            this.weightedMedian = t.f0;
        }
    }

    @Override
    public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple3<Double, Integer, Integer> t) throws Exception {
        return new Tuple5<>(t.f0 < weightedMedian ? 1 : 0, t.f0 == weightedMedian ? 1 : 0, t.f0 > weightedMedian ? 1 : 0, t.f1, t.f2);
    }

}