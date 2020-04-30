package clusterdata.exercise3;

import clusterdata.datatypes.TaskEvent;
import clusterdata.sources.TaskEventSource;
import clusterdata.utils.AppBase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

class InvTask {
    public int priority;
    public long duration;
    public long start;
    public long end;

    public static InvTask of(int priority, long duration, long start, long end) {
        InvTask result = new InvTask();
        result.priority = priority;
        result.duration = duration;
        result.start = start;
        result.end = end;
        return result;
    }
}

public class InvariantBasedAnomaly extends AppBase {
    private static long trainingEnd;
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.get("input", pathToTaskEventData);

        trainingEnd = params.getLong("training-end");

        // events of 10 minute are served in 1 second
        final int servingSpeedFactor = 60000;

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<TaskEvent> taskEvents = env
                .addSource(taskSourceOrTest(new TaskEventSource(input, servingSpeedFactor)))
                .setParallelism(1);


        DataStream<Tuple2<Integer, Long>> res = taskEvents
                .filter(new MyFilteredFunction())
                .keyBy("jobId", "taskIndex")
                .process(new MyProcessFunction())
                .flatMap(new MyFlatMapFunction())
                .keyBy(0)
                .process(new MyKeyed1ProcessFunction());

        printOrTest(res);
        env.execute("Invariant-Based Anomaly Test");
    }

    public static class MyFilteredFunction implements FilterFunction<TaskEvent> {

        @Override
        public boolean filter(TaskEvent taskEvent) throws Exception {
            return taskEvent.eventType.getValue() == 0 || taskEvent.eventType.getValue() == 1;
        }
    }

    public static class MyProcessFunction extends KeyedProcessFunction<Tuple, TaskEvent, InvTask> {
        private ValueState<TaskEvent> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("state", TaskEvent.class));
        }

        @Override
        public void processElement(TaskEvent taskEvent, Context ctx, Collector<InvTask> out) throws Exception {
            TaskEvent cur = state.value();
            if (cur != null) {
                if (cur.eventType != taskEvent.eventType) {
                    state.clear();
                    long duration = Math.abs(taskEvent.timestamp - cur.timestamp);
                    out.collect(InvTask.of(taskEvent.priority, duration, taskEvent.timestamp, cur.timestamp));
                } else {
                    state.update(taskEvent);
                }
            } else {
                state.update(taskEvent);
            }
        }
    }

    public static class MyFlatMapFunction implements FlatMapFunction<InvTask, Tuple2<Integer, InvTask>> {

        @Override
        public void flatMap(InvTask invTask, Collector<Tuple2<Integer, InvTask>> out) throws Exception {
            out.collect(new Tuple2<>(invTask.priority, invTask));
        }
    }

    public static class MyKeyed1ProcessFunction extends KeyedProcessFunction<Tuple, Tuple2<Integer, InvTask>, Tuple2<Integer, Long>> {
        private ValueState<Long> minState;
        private ValueState<Long> maxState;
        private ValueState<InvTask> curState;
        private ListState<InvTask> aheadState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Long> min = new ValueStateDescriptor<>(
                    "min", Long.class);
            ValueStateDescriptor<Long> max = new ValueStateDescriptor<>(
                    "max", Long.class);
            ValueStateDescriptor<InvTask> curr = new ValueStateDescriptor<>(
                    "cur", InvTask.class);
            ListStateDescriptor<InvTask> ahead = new ListStateDescriptor<>(
                    "ahead", InvTask.class);
            minState = getRuntimeContext().getState(min);
            maxState = getRuntimeContext().getState(max);
            curState = getRuntimeContext().getState(curr);
            aheadState = getRuntimeContext().getListState(ahead);
        }
        @Override
        public void processElement(Tuple2<Integer, InvTask> in, Context ctx, Collector<Tuple2<Integer, Long>> out) throws Exception {
            long watermark = ctx.timerService().currentWatermark();
            long start = in.f1.start;

            // find that if the pair matched is happened after the watermark and trainingEnd
            if (watermark < trainingEnd && start > trainingEnd) {
                aheadState.add(in.f1);
            }

            if (watermark < trainingEnd) {
                if (minState.value() != null) {
                    if (minState.value() > in.f1.duration) {
                        minState.update(in.f1.duration);
                    }
                } else {
                    minState.update(in.f1.duration);
                }

                if (maxState.value() != null) {
                    if (maxState.value() < in.f1.duration) {
                        maxState.update(in.f1.duration);
                    }
                } else {
                    maxState.update(in.f1.duration);
                }
            }

            if (watermark > trainingEnd) {
                curState.update(in.f1);
                ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 1);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Integer, Long>> out) throws Exception {
            long min = minState.value();
            long max = maxState.value();
            // output ahead state for only one time
            if (aheadState != null) {
                for (InvTask t : aheadState.get()) {
                    if (t.duration > max || t.duration < min) {
                        out.collect(new Tuple2<>(t.priority, t.duration));
                    }
                }
                aheadState.clear();
            }

            InvTask c = curState.value();
            curState.clear();
            if (c != null) {
                if (c.duration > max || c.duration < min) {
                    out.collect(new Tuple2<>(c.priority, c.duration));
                }
            }
        }
    }
}
