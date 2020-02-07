package clusterdata.exercise1;

import clusterdata.datatypes.TaskEvent;
import clusterdata.utils.AppBase;
import clusterdata.utils.TaskEventSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * This programs reads the filtered TaskEvents back from Kafka and computes the maximum task duration per priority.
 *
 * Parameters:
 * --input path-to-input-file
 */
public class MaxTaskCompletionTimeFromKafka extends AppBase {

    private static final String LOCAL_ZOOKEEPER_HOST = "localhost:2181";
    private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
    private static final String TASKS_GROUP = "taskGroup";

    public static void main(String[] args) throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // do not worry about the following two configuration options for now
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        // set the parallelism
        // TODO: check that your program works correctly with higher parallelism
        env.setParallelism(1);

        //TODO: configure the Kafka consumer
        // always read the Kafka topic from the start
        Properties kafkaProps = new Properties();

        kafkaProps.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST); // Zookeeper default host:port
        kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER); // Broker default host:port
        kafkaProps.setProperty("group.id", TASKS_GROUP);                 // Consumer group ID
        kafkaProps.setProperty("auto.offset.reset", "earliest");       // Always read topic from start

        //TODO: implement the following transformations
        // create a Kafka consumer
        // FlinkKafkaConsumer011<TaskEvent> consumer = ...
        // assign a timestamp extractor to the consumer (provided below)
        // consumer.assignTimestampsAndWatermarks(new TSExtractor());
        // create the data stream
        // DataStream<TaskEvent> events = env.addSource(taskSourceOrTest(consumer));
        // compute the durations per task
        // DataStream<Tuple2<Integer, Long>> taskDurations = events...
        // output the maximum duration per priority in 2-tuples of (priority, duration)
        // DataStream<Tuple2<Integer, Long>> maxDurationsPerPriority = taskDurations
        // printOrTest(maxDurationsPerPriority);
        FlinkKafkaConsumer011<TaskEvent> consumer = new FlinkKafkaConsumer011<>(
                        "cleansedRides", new TaskEventSchema(), kafkaProps);
        consumer.assignTimestampsAndWatermarks(new TSExtractor());
        DataStream<TaskEvent> events = env.addSource(taskSourceOrTest(consumer));
        DataStream<Tuple2<Integer, Long>> taskDurations = events.
        DataStream<Tuple2<Integer, Long>> maxDurationsPerPriority = taskDurations.
        env.execute();
    }

    /**
     * Assigns timestamps to TaskEvent records.
     * Watermarks are a fixed time interval behind the max timestamp and are periodically emitted.
     */
    public static class TSExtractor extends BoundedOutOfOrdernessTimestampExtractor<TaskEvent> {

        public TSExtractor() {
            super(Time.seconds(0));
        }

        @Override
        public long extractTimestamp(TaskEvent event) {
           return event.timestamp;
        }
    }
}
