package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;

/**
 * @author yingkf
 * @date 2023年02月16日11:41:22
 */
public class Enrichment implements MapFunction<TaxiRide, EnrichedRide> {
    @Override
    public EnrichedRide map(TaxiRide value) throws Exception {
        return new EnrichedRide(value);
    }
}
