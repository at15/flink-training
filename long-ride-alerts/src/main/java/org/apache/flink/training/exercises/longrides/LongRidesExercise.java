/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.longrides;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * The "Long Ride Alerts" exercise.
 *
 * <p>The goal for this exercise is to emit the rideIds for taxi rides with a duration of more than
 * two hours. You should assume that TaxiRide events can be lost, but there are no duplicates.
 *
 * <p>You should eventually clear any state you create.
 */
public class LongRidesExercise {
    private final SourceFunction<TaxiRide> source;
    private final SinkFunction<Long> sink;

    /**
     * Creates a job using the source and sink provided.
     */
    public LongRidesExercise(SourceFunction<TaxiRide> source, SinkFunction<Long> sink) {
        this.source = source;
        this.sink = sink;
    }

    /**
     * Creates and executes the long rides pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(source);

        // the WatermarkStrategy specifies how to extract timestamps and generate watermarks
        WatermarkStrategy<TaxiRide> watermarkStrategy =
                WatermarkStrategy.<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner(
                                (ride, streamRecordTimestamp) -> ride.getEventTimeMillis());

        // create the pipeline
        rides.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(ride -> ride.rideId)
                .process(new AlertFunction())
                .addSink(sink);

        // execute the pipeline and return the result
        return env.execute("Long Taxi Rides");
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        LongRidesExercise job =
                new LongRidesExercise(new TaxiRideGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    // <Long, TaxiRide, Long>
    // <KeyType, StreamItemType, OutputType>
    @VisibleForTesting
    public static class AlertFunction extends KeyedProcessFunction<Long, TaxiRide, Long> {
        // key is RideId, value is Timestamp
        private transient MapState<Long, Long> startTimes;
        private transient MapState<Long, Long> endTimes;

        @Override
        public void open(Configuration config) throws Exception {
            MapStateDescriptor<Long, Long> startTimesDesc = new MapStateDescriptor<>("startTimes", Long.class, Long.class);
            MapStateDescriptor<Long, Long> endTimesDesc = new MapStateDescriptor<>("endTimes", Long.class, Long.class);
            startTimes = getRuntimeContext().getMapState(startTimesDesc);
            endTimes = getRuntimeContext().getMapState(endTimesDesc);
        }

        @Override
        public void processElement(TaxiRide ride, Context context, Collector<Long> out)
                throws Exception {
            TimerService timerService = context.timerService();
            if (ride.isStart) {
                // START
                long startTime = ride.getEventTimeMillis();

                // Have we seen the END?
                Long endTime = endTimes.get(ride.rideId);
                if (endTime != null) {
                    // Compare
                    tryAlert(startTime, endTime, out, ride.rideId);
                    // Clear State
                    endTimes.remove(ride.rideId);
                } else {
                    // Save START
                    startTimes.put(ride.rideId, startTime);
                    long twoHoursLater = startTime + TWO_HOURS;
                    timerService.registerEventTimeTimer(twoHoursLater);
                }
            } else {
                // END
                long endTime = ride.getEventTimeMillis();

                // Have we seen the START?
                Long startTime = startTimes.get(ride.rideId);
                if (startTime != null) {
                    // Compare
                    tryAlert(startTime, endTime, out, ride.rideId);
                    // Clear State
                    startTimes.remove(ride.rideId);
                    long twoHoursLater = startTime + TWO_HOURS;
                    timerService.deleteEventTimeTimer(twoHoursLater); // NOTE: learned from ut, I need to remove the timer.
                } else {
                    // Save END
                    endTimes.put(ride.rideId, endTime);
                    // TODO: we still need timer, although START always arrives, its 2h timer will remove startTime
                    // from state. If END arrives after that e.g. 2h+1s, no one will clear the state..
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Long> out)
                throws Exception {
            // TODO: why I cannot pass more context when registering timer? have to use state?
            long rideId = context.getCurrentKey();
            Long startTime = startTimes.get(rideId);
            if (startTime != null) {
                // Alert because END did not arrive in time, otherwise startTime should be null
                out.collect(rideId);
                startTimes.remove(rideId);
            }
            // END arrived before timer fired.
        }

        private static final long TWO_HOURS = TimeUnit.HOURS.toMillis(2);

        private void tryAlert(long start, long end, Collector<Long> out, long rideId) {
            if (moreThan2Hour(start, end)) {
                out.collect(rideId);
            }
        }

        private boolean moreThan2Hour(long start, long end) {
            if (start > end) {
                throw new RuntimeException("start is later than end");
            }
            return (end - start) > TWO_HOURS;
        }
    }
}
