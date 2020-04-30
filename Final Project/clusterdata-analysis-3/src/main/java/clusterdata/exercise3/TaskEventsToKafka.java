package clusterdata.exercise3;

import clusterdata.datatypes.TaskEvent;
import clusterdata.sources.TaskEventSource;
import clusterdata.utils.AppBase;
import clusterdata.utils.TaskEventSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class TaskEventsToKafka extends AppBase{
    private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
    public static final String FT_TASKS_TOPIC = "ft";

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

        taskEvents.addSink((SinkFunction<TaskEvent>) sinkOrTest(new FlinkKafkaProducer011<TaskEvent>(
                LOCAL_KAFKA_BROKER,          // Kafka broker host:port
                FT_TASKS_TOPIC,       // Topic to write to
                new TaskEventSchema())));   // Serializer (provided as util
        // run the cleansing pipeline
        env.execute("Task Events to Kafka");
    }
}
