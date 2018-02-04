package de.lwerner.flink.percentiles.data;

import de.lwerner.flink.percentiles.model.Result;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract sink to provide methods to all sinks
 *
 * @author Lukas Werner
 */
public abstract class AbstractSink implements SinkInterface {

    /**
     * The application logger
     */
    protected Logger logger;

    /**
     * Constructor to initialize logger
     */
    public AbstractSink() {
        logger = LoggerFactory.getLogger(getClass());
    }

    /**
     * Map solution data set to a tuple data set with all information
     *
     * @param result the result model
     *
     * @return data set with all information
     */
    protected DataSet<Tuple4<Long, Integer, Long, Float>> solutionDataSetToTuple(Result result) {
        return result.getSolution().map(new SolutionToTupleMap(result.getK(), result.getP(), result.getT()));
    }

    /**
     * Inner class because it is only used here for sure
     *
     * @author Lukas Werner
     */
    private static class SolutionToTupleMap implements MapFunction<Tuple1<Float>, Tuple4<Long, Integer, Long, Float>> {

        /**
         * k
         */
        private long k;
        /**
         * p
         */
        private int p;
        /**
         * t
         */
        private long t;

        /**
         * Constructor to set all values
         *
         * @param k k to set
         * @param p p to set
         * @param t t to set
         */
        public SolutionToTupleMap(long k, int p, long t) {
            this.k = k;
            this.p = p;
            this.t = t;
        }

        @Override
        public Tuple4<Long, Integer, Long, Float> map(Tuple1<Float> value) {
            return new Tuple4<>(k, p, t, value.f0);
        }

    }

}