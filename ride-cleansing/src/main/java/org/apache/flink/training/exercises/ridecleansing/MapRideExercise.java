package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;

/**
 * @author yingkf
 * @date 2023年02月16日11:39:27
 */
public class MapRideExercise {
    public static void main(String[] args) throws Exception {
        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the pipeline
        DataStream<EnrichedRide> enrichedRides = env.addSource(new TaxiRideGenerator())
                .filter(new RideCleansingExercise.NYCFilter()).map(new Enrichment());
        enrichedRides.print();
        env.execute("Taxi Ride cleaning and enriched");
        // run the pipeline and return the result
        // return env.execute("Taxi Ride Cleansing");
    }
}
