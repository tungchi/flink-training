package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;

/**
 * @author yingkf
 * @date 2023年02月16日13:34:56
 */
public class FlatMapRideExercise {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // set up the pipeline
        DataStream<EnrichedRide> enrichedRides =
                env.addSource(new TaxiRideGenerator()).flatMap(new NYCEnrichment());
        enrichedRides.print();
        env.execute("Taxi Ride cleaning and enriched");
    }
}
