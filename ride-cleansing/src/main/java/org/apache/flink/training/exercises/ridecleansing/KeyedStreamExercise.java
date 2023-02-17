package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.util.Collector;
import org.joda.time.Interval;
import org.joda.time.Minutes;

/**
 * @author yingkf
 * @date 2023年02月16日13:58:46
 */
public class KeyedStreamExercise {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<EnrichedRide> enrichedRides =
                env.addSource(new TaxiRideGenerator()).flatMap(new NYCEnrichment());
        DataStream<Tuple2<Integer, Minutes>> minutesByStartCell = enrichedRides
                .flatMap(new FlatMapFunction<EnrichedRide, Tuple2<Integer, Minutes>>() {
                    @Override
                    public void flatMap(EnrichedRide ride, Collector<Tuple2<Integer, Minutes>> out)
                            throws Exception {
                        if (!ride.isStart) {
                            Interval rideInterval = new Interval(ride.startTime.toEpochMilli(),
                                    ride.endTime.toEpochMilli());
                            Minutes duration = rideInterval.toDuration().toStandardMinutes();
                            out.collect(new Tuple2<>(ride.startCell, duration));
                        }
                    }
                });
        // minutesByStartCell.print();
        minutesByStartCell.keyBy(value -> value.f0).maxBy(1).print();
        env.execute();

    }
}
