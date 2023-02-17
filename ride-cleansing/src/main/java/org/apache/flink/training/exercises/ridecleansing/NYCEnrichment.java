package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.util.Collector;

/**
 * @author yingkf
 * @date 2023年02月16日13:36:09
 */
public class NYCEnrichment implements FlatMapFunction<TaxiRide, EnrichedRide> {
    @Override
    public void flatMap(TaxiRide ride, Collector<EnrichedRide> out) throws Exception {
        FilterFunction<TaxiRide> valid = new RideCleansingExercise.NYCFilter();
        if (valid.filter(ride)) {
            out.collect(new EnrichedRide(ride));
        }
    }
}
