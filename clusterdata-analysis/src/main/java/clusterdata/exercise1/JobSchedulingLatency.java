package clusterdata.exercise1;

import clusterdata.datatypes.EventType;
import clusterdata.datatypes.JobEvent;
import clusterdata.sources.JobEventSource;
import clusterdata.utils.AppBase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

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
        final int servingSpeedFactor = 60;

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure the time characteristic; don't worry about this yet
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // set the parallelism
        // TODO: check that your program works correctly with higher parallelism
        env.setParallelism(1);

        // start the data generator
        DataStream<JobEvent> events = env
                .addSource(jobSourceOrTest(new JobEventSource(input, servingSpeedFactor)))
                .setParallelism(1);

        //TODO: implement the following transformations
        // Filter events and only keep submit and schedule
        // DataStream<JobEvent> filteredEvents = ...
        // The results stream consists of tuple-2 records, where field 0 is the jobId and field 1 is the job duration
        // DataStream<Tuple2<Long, Long>> jobIdWithLatency = ...
        // printOrTest(jobIdWithLatency);
        DataStream<JobEvent> filteredEvents = events.filter(new FilterFunction<JobEvent>() {
            @Override
            public boolean filter(JobEvent jobEvent) throws Exception {
                if (jobEvent.eventType.getValue() == 0 || jobEvent.eventType.getValue() == 1) {
                    return true;
                } else {
                    return false;
                }
            }
        });

        DataStream<Tuple2<Long, Long>> jobIdWithLatency = filteredEvents
                .keyBy("jobId")
                .flatMap(new DurationMapping());

        printOrTest(jobIdWithLatency);
        // execute the dataflow
        env.execute("Job Scheduling Latency Application");
    }

    private static final class DurationMapping implements FlatMapFunction<JobEvent, Tuple2<Long, Long>> {
        Map<Long, JobEvent> map;
        public void flatMap(JobEvent jobEvent, Collector<Tuple2<Long, Long>> collector) throws Exception {
            // no jobid is in the map, means no schedule or submit event
            if (map == null) {
                map = new HashMap<>();
            }
            // three situations: submit -> submit, submit -> schedule, schedule -> submit
            if (!map.containsKey(jobEvent.jobId)) {
                // no event neither schedule or submit
                map.put(jobEvent.jobId, jobEvent);
            } else {
                // exist schedule submit pair
                // multiple submit, remove old submit, put new submit, keep only one submit in hashmap
                if (map.get(jobEvent.jobId).eventType.getValue() == 0 && jobEvent.eventType.getValue() == 0) {
                    map.remove(jobEvent.jobId);
                    map.put(jobEvent.jobId, jobEvent);
                }
                long time = map.get(jobEvent.jobId).timestamp;
                long time2 = jobEvent.timestamp;
                collector.collect(new Tuple2<>(jobEvent.jobId, Math.abs(time - time2)));
            }
        }
    }
}
