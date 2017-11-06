package de.lwerner.flink.percentiles.functions.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Function, which maps each double value, which is input to the function to a Tuple
 *
 * @author Lukas Werner
 */
public class InputToTupleMapFunction implements MapFunction<Double, Tuple3<Double, Integer, Integer>> {

    /**
     * k
     */
    private int k;
    /**
     * n
     */
    private int n;

    /**
     * Constructor sets k and n to map
     *
     * @param k the k
     * @param n the n
     */
    public InputToTupleMapFunction(int k, int n) {
        this.k = k;
        this.n = n;
    }

    @Override
    public Tuple3<Double, Integer, Integer> map(Double d) throws Exception {
        return new Tuple3<>(d, k, n);
    }

}