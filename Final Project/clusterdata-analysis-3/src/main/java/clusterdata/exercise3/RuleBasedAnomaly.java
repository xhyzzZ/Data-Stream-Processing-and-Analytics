package clusterdata.exercise3;

import clusterdata.datatypes.TaskEvent;
import clusterdata.sources.TaskEventSource;
import clusterdata.utils.AppBase;
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
import org.apache.flink.util.Collector;

public class RuleBasedAnomaly extends AppBase {
    private static int eventType1;
    private static int eventType2;
    private static int eventType3;
    private static long timeConstraint;
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.get("input", pathToTaskEventData);

        // events of 10 minute are served in 1 second
        final int servingSpeedFactor = 60000;

        eventType1 = params.getInt("e1");
        eventType2 = params.getInt("e2");
        eventType3 = params.getInt("e3");
        timeConstraint = params.getLong("time-constraint");

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        DataStream<TaskEvent> taskEvents = env
                .addSource(taskSourceOrTest(new TaskEventSource(input, servingSpeedFactor)))
                .setParallelism(1);

        DataStream<Tuple3<TaskEvent, TaskEvent, TaskEvent>> res = taskEvents
                .filter(new MyFilteredFunction())
                .keyBy("jobId", "taskIndex")
                .process(new MyKeyProcessFunction());

        printOrTest(res);
        env.execute("Rule-Based Anomaly Test");
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
