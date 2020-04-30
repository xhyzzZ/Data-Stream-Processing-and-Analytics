package clusterdata.exercise3;

import clusterdata.datatypes.TaskEvent;
import clusterdata.utils.AppBase;
import clusterdata.utils.TaskEventSchema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class FaultTolerance extends AppBase {
    private static int eventType1;
    private static int eventType2;
    private static int eventType3;
    private static long timeConstraint;
    private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
    private static final String LOCAL_ZOOKEEPER_HOST = "localhost:2181";
    private static final String TASKS_GROUP = "taskGroup";

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        eventType1 = params.getInt("e1");
        eventType2 = params.getInt("e2");
        eventType3 = params.getInt("e3");
        timeConstraint = params.getLong("time-constraint");

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // do not worry about the following two configuration options for now
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);
        
        // checkpoint every 5000 msecs or 1000
        env.enableCheckpointing(1000);

        //TODO: configure the Kafka consumer
        // always read the Kafka topic from the start
        Properties kafkaProps = new Properties();

        kafkaProps.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST); // Zookeeper default host port
        kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER); // Broker default host port
        kafkaProps.setProperty("group.id", TASKS_GROUP);                 // Consumer group ID
        // kafka property doesn't work in this version.
//        kafkaProps.setProperty("auto.offset.reset", "earliest");       // Read topic from start

        FlinkKafkaConsumer011<TaskEvent> consumer = new FlinkKafkaConsumer011<>(
                TaskEventsToKafka.FT_TASKS_TOPIC,
                new TaskEventSchema(),
                kafkaProps);
        
        // set the property from earliest using API
        consumer.setStartFromEarliest();
        consumer.assignTimestampsAndWatermarks(new TSExtractor());

        DataStream<TaskEvent> events = env
                .addSource(taskSourceOrTest(consumer))
                .uid("source-id");

        DataStream<Tuple3<TaskEvent, TaskEvent, TaskEvent>> res = events
                .filter(new MyFilteredFunction())
                .uid("filter-id")
                .keyBy("jobId", "taskIndex")
                .process(new MyKeyProcessFunction())
                .uid("process-id");

        printOrTest(res);
        env.execute("Fault-Tolerance");
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

    public static class MyFilteredFunction implements FilterFunction<TaskEvent> {

        @Override
        public boolean filter(TaskEvent in) throws Exception {
            return in.eventType.getValue() == eventType1
                    || in.eventType.getValue() == eventType2
                    || in.eventType.getValue() == eventType3;
        }
    }

    public static class MyKeyProcessFunction extends KeyedProcessFunction<Tuple, TaskEvent, Tuple3<TaskEvent, TaskEvent, TaskEvent>> {
        private MapState<Integer, TaskEvent> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            MapStateDescriptor<Integer, TaskEvent> itemsStateDesc = new MapStateDescriptor<>(
                    "state", Integer.class, TaskEvent.class);
            state = getRuntimeContext().getMapState(itemsStateDesc);
        }

        @Override
        public void processElement(TaskEvent in, Context ctx, Collector<Tuple3<TaskEvent, TaskEvent, TaskEvent>> out) throws Exception {
            int key = in.eventType.getValue();
            // for the first time we meet all three types of event
            if (!state.contains(key)) {
                // when we first time meet the starting type(smallest value),
                // we set timer for the scope(t, t + time-constraint)
                if (key == eventType1) {
                    ctx.timerService().registerEventTimeTimer(in.timestamp + timeConstraint);
                }
                state.put(key, in);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<TaskEvent, TaskEvent, TaskEvent>> out) throws Exception {
            TaskEvent start = state.get(eventType1);
            TaskEvent mid = state.get(eventType2);
            TaskEvent end = state.get(eventType3);

            // clear state data
            state.clear();
            // in the scope we have these three types, then choose to output
            if (end != null && start != null && mid != null) {
                if (start.timestamp < mid.timestamp && mid.timestamp < end.timestamp) {
                    if (end.timestamp - start.timestamp <= timeConstraint) {
                        out.collect(new Tuple3<>(start, mid, end));
                    }
                }
            }
        }
    }
}
