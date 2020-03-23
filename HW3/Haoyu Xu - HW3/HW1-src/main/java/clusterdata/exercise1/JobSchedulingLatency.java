package clusterdata.exercise1;

import clusterdata.datatypes.JobEvent;
import clusterdata.sources.JobEventSource;
import clusterdata.utils.AppBase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

/**
 * Measure the time between submitting and scheduling each job in the cluster.
 * For each jobID, record the timestamp of the SUBMIT(0) event and look for a subsequent SCHEDULE(1) event.
 * Once received, output their time difference.
 *
 * Note: If a job is submitted multiple times, then measure the latency since the first submission.
 *
 *  Parameters:
 *  --input path-to-input-file
 */
public class JobSchedulingLatency extends AppBase {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", AppBase.pathToJobEventData);

        // events of 1 minute are served in 1 second
        // TODO: you can play around with different speed factors
        final int servingSpeedFactor = 60000;

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        // set latency tracking every 500ms
//        env.getConfig().setLatencyTrackingInterval(500);

        // Configure the time characteristic; don't worry about this yet
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        // set the parallelism
        // TODO: check that your program works correctly with higher parallelism
        env.setParallelism(1);

        // start the data generator
        // random data missing with high parallelism, due to small size of dataset and high loading speed
        DataStream<JobEvent> events = env
                .addSource(jobSourceOrTest(new JobEventSource(input, servingSpeedFactor)))
                .setParallelism(1);

        //TODO: implement the following transformations
        // Filter events and only keep submit and schedule
        // DataStream<JobEvent> filteredEvents = ...
        // The results stream consists of tuple-2 records, where field 0 is the jobId and field 1 is the job duration
        // DataStream<Tuple2<Long, Long>> jobIdWithLatency = ...
        // printOrTest(jobIdWithLatency);
        DataStream<JobEvent> filteredEvents = events.filter(new MyFilteredFuntion());

        DataStream<Tuple2<Long, Long>> jobIdWithLatency = filteredEvents
                .keyBy("jobId")
                .process(new MyProcessFuntion());
//        jobIdWithLatency.addSink(new SinkFunction<Tuple2<Long, Long>>() {
//            @Override
//            public void invoke(Tuple2<Long, Long> value, Context context) throws Exception {
//                // no-op sink
//            }
//        });
        printOrTest(jobIdWithLatency);
        // execute the dataflow
        env.execute("Job Scheduling Latency Application");
    }

    public static class MyFilteredFuntion implements FilterFunction<JobEvent> {
        @Override
        public boolean filter(JobEvent jobEvent) throws Exception {
            if (jobEvent.eventType.getValue() == 0 || jobEvent.eventType.getValue() == 1) {
                return true;
            } else {
                return false;
            }
        }
    }

    public static class MyProcessFuntion extends KeyedProcessFunction<Tuple, JobEvent, Tuple2<Long, Long>> {
        private ValueState<JobEvent> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("state", JobEvent.class));
        }

        @Override
        public void processElement(JobEvent jobEvent, Context ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
            JobEvent cur = state.value();
            if (cur != null) {
                if (cur.eventType != jobEvent.eventType) {
                    state.clear();
                    long duration = Math.abs(jobEvent.timestamp - cur.timestamp);
                    out.collect(new Tuple2<>(jobEvent.jobId, duration));
                } else {
                    state.update(jobEvent);
                }
            } else {
                state.update(jobEvent);
            }
        }
    }
}
