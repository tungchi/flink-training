package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.utils.GeoUtils;

/**
 * @author yingkf
 * @date 2023年02月16日11:36:15
 */
public class EnrichedRide extends TaxiRide {
    public int startCell;
    public int endCell;

    public EnrichedRide(TaxiRide taxiRide) {
        this.rideId = taxiRide.rideId;
        this.isStart = taxiRide.isStart;
        this.eventTime = taxiRide.eventTime;
        this.startLon = taxiRide.startLon;
        this.startLat = taxiRide.startLat;
        this.endLon = taxiRide.endLon;
        this.endLat = taxiRide.endLat;
        this.passengerCnt = taxiRide.passengerCnt;
        this.taxiId = taxiRide.taxiId;
        this.driverId = taxiRide.driverId;
        this.startCell = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
        this.endCell = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);

        this.startTime = taxiRide.startTime;
        this.endTime = taxiRide.endTime;
    }

    public String toString() {
        return super.toString() + "," + this.startCell + "," + this.endCell;
    }
}
