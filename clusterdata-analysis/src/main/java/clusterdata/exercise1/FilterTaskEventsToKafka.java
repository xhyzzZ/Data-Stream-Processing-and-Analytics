package clusterdata.exercise1;

import clusterdata.datatypes.JobEvent;
import clusterdata.datatypes.TaskEvent;
import clusterdata.sources.TaskEventSource;
import clusterdata.utils.AppBase;
import clusterdata.utils.TaskEventSchema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * Write SUBMIT(0) and FINISH(4) TaskEvents to a Kafka topic.
 * Make sure to start Kafka and create the topic before running this application!
 *
 * Parameters:
 * --input path-to-input-file
 *
 */
public class FilterTaskEventsToKafka extends AppBase {

    private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
    public static final String FILTERED_TASKS_TOPIC = "filteredTasks";

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.get("input", pathToTaskEventData);

        // events of 110 minutes are served in 1 second
        // TODO: you can play around with different speed factors
        final int servingSpeedFactor = 600;

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // set the parallelism
        // TODO: check that your program works correctly with higher parallelism
        env.setParallelism(1);

        // start the data generator
        DataStream<TaskEvent> taskEvents = env
                .addSource(taskSourceOrTest(new TaskEventSource(input, servingSpeedFactor)))
                .setParallelism(1);

        //TODO: implement the following transformations
        // DataStream<TaskEvent> filteredEvents = ...
        // write the filtered data to a Kafka sink
        // filteredEvents.addSink((SinkFunction<TaskEvent>) sinkOrTest(new FlinkKafkaProducer011<TaskEvent>(...)));
        DataStream<TaskEvent> filteredEvents = taskEvents.filter(new FilterFunction<TaskEvent>() {
            @Override
            public boolean filter(TaskEvent taskEvent) throws Exception {
                if (taskEvent.eventType.getValue() == 0 || taskEvent.eventType.getValue() == 4) {
                    return true;
                } else {
                    return false;
                }
            }
        });

        filteredEvents.addSink(new FlinkKafkaProducer011<>(
                LOCAL_KAFKA_BROKER,      // Kafka broker host:port
                FILTERED_TASKS_TOPIC,    // Topic to write to
                new TaskEventSchema())  // Serializer (provided as util)
        );

        // run the cleansing pipeline
        env.execute("Task Events to Kafka");
    }

}