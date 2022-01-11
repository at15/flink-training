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

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.util.Collector;

/**
 * The Hourly Tips exercise from the Flink training.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour
 * by hour, and then from that stream, find the highest tip total in each hour.
 */
public class HourlyTipsExercise {

    private final SourceFunction<TaxiFare> source;
    private final SinkFunction<Tuple3<Long, Long, Float>> sink;

    /**
     * Creates a job using the source and sink provided.
     */
    public HourlyTipsExercise(
            SourceFunction<TaxiFare> source, SinkFunction<Tuple3<Long, Long, Float>> sink) {

        this.source = source;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        HourlyTipsExercise job =
                new HourlyTipsExercise(new TaxiFareGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    /**
     * Create and execute the hourly tips pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        DataStream<TaxiFare> fares = env.addSource(source)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TaxiFare>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.getEventTimeMillis()));

        // replace this with your solution
        DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
                .keyBy(fare -> fare.driverId)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .reduce((f1, f2) -> {
                    f1.tip += f2.tip;
                    return f1;
                }, new HourTip());

        //
        // DataStream<Tuple3<Long, Long, Float>> hourlyMax = fares
        //         .keyBy(fare -> fare.driverId)
        //         .window(TumblingEventTimeWindows.of(Time.hours(1)))
        //         .reduce((f1, f2) -> {
        //             f1.tip += f2.tip;
        //             return f1;
        //         })// group by driver id, sum it every hour
        // NOTE: discussion mentioned why it's good to use window instead of key by timestamp
        //         // .keyBy(fare -> fare.getEventTimeMillis() / 1000 / 3600)// group by hour, find the max
        //         .windowAll(TumblingEventTimeWindows.of(Time.hours(1))) // flush result every 1 hour
        //         .reduce((f1, f2) -> f1.tip > f2.tip ? f1 : f2, new LastIsMaxAll()); // same as https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/learn-flink/streaming_analytics/#incremental-aggregation-example
        // .reduce((f1, f2) -> f1.tip > f2.tip ? f1 : f2)
        // .map(f -> {
        //     long hour = f.getEventTimeMillis() / 1000 / 3600;
        //     return Tuple3.of(hour, f.driverId, f.tip);
        // });
        DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .reduce((t1, t2) -> t1.f2 > t2.f2 ? t1 : t2);

        // the results should be sent to the sink that was passed in
        // (otherwise the tests won't work)
        // you can end the pipeline with something like this:

        // DataStream<Tuple3<Long, Long, Float>> hourlyMax = ...
        hourlyMax.addSink(sink);

        // execute the pipeline and return the result
        return env.execute("Hourly Tips");
    }

    public static class HourTip extends ProcessWindowFunction<
            TaxiFare,
            Tuple3<Long, Long, Float>,
            Long,
            TimeWindow> {

        @Override
        public void process(Long key,
                            Context context,
                            Iterable<TaxiFare> elements,
                            Collector<Tuple3<Long, Long, Float>> out) throws Exception {
            TaxiFare sum = elements.iterator().next();
            // long hour = maxOne.getEventTimeMillis() / 1000 / 3600;
            // NOTE: key is same as sum.driverId
            out.collect(Tuple3.of(context.window().getEnd(), sum.driverId, sum.tip));
        }
    }

    public static class LastIsMaxAll extends ProcessAllWindowFunction<
            TaxiFare,
            Tuple3<Long, Long, Float>,
            TimeWindow> {

        @Override
        public void process(Context context,
                            Iterable<TaxiFare> elements,
                            Collector<Tuple3<Long, Long, Float>> out) throws Exception {
            TaxiFare maxOne = elements.iterator().next();
            long hour = maxOne.getEventTimeMillis() / 1000 / 3600;
            out.collect(Tuple3.of(hour, maxOne.driverId, maxOne.tip));
        }
    }
}
