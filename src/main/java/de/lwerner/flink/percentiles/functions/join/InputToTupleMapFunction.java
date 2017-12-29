package de.lwerner.flink.percentiles.functions.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Function, which maps each double value, which is input to the function to a Tuple
 *
 * @author Lukas Werner
 */
public class InputToTupleMapFunction implements MapFunction<Tuple1<Float>, Tuple3<Float, Long, Long>> {

    /**
     * k
     */
    private long k;
    /**
     * n
     */
    private long n;

    /**
     * Constructor sets k and n to map
     *
     * @param k the k
     * @param n the n
     */
    public InputToTupleMapFunction(long k, long n) {
        this.k = k;
        this.n = n;
    }

    @Override
    public Tuple3<Float, Long, Long> map(Tuple1<Float> t) {
        return new Tuple3<>(t.f0, k, n);
    }

}