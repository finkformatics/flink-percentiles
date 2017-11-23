package de.lwerner.flink.percentiles.functions.redis;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * Partitions data randomly
 *
 * @author Lukas Werner
 */
public class RandomPartitioner implements Partitioner<Float> {

    @Override
    public int partition(Float key, int numPartitions) {
        return (int)(Math.random() * 12329) % numPartitions;
    }

}