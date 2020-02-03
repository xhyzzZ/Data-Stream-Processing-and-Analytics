package clusterdata.exercise1;

import clusterdata.datatypes.JobEvent;
import clusterdata.sources.JobEventSource;
import clusterdata.utils.AppBase;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * This application continuously outputs the number of events per jobId seen so far in the job events stream.
 */
public class JobEventCount extends AppBase {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", AppBase.pathToJobEventData);

        final int servingSpeedFactor = 60; // events of 1 minute are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure the time characteristic; don't worry about this yet
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // set the parallelism
        env.setParallelism(1);

        // start the data generator
        DataStream<JobEvent> events = env.addSource(jobSourceOrTest(new JobEventSource(input, servingSpeedFactor)));

        // map each job event to a 2-tuple
        DataStream<Tuple2<Long, Long>> mappedEvents =  events.map(new AppendOneMapper());

        // group the stream of tuples by jobId
        KeyedStream<Tuple2<Long, Long>, Tuple> keyedEvents = mappedEvents.keyBy(0);

        // sum all the 1s and print the result stream
        DataStream<Tuple2<Long, Long>> result = keyedEvents.sum(1);

        printOrTest(result);

        // execute the dataflow
        env.execute("Continuously count job events");
    }

    /**
     * A helper class that implements a map transformation.
     * For each input JobEvent record, the mapper output a tuple-2 containing the jobId as the first field
     * and the number 1 as the second field.
     */
    private static final class AppendOneMapper implements MapFunction<JobEvent, Tuple2<Long, Long>> {
        @Override
        public Tuple2<Long, Long> map(JobEvent jobEvent) throws Exception {
            return new Tuple2<>(jobEvent.jobId, 1L);
        }
    }
}

