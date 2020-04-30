package clusterdata.exercise3;

import clusterdata.datatypes.TaskEvent;
import clusterdata.sources.TaskEventSource;
import clusterdata.utils.AppBase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

class TimeTask {
    public long key;
    public double curAvg;
    public long timestamp;

    public static TimeTask of(long key, double curAvg, long timestamp) {
        TimeTask result = new TimeTask();
        result.key = key;
        result.curAvg = curAvg;
        result.timestamp = timestamp;
        return result;
    }
}

public class TimeSeriesAnomaly extends AppBase {
    private static long length;
    private static long slide;
    private static int numberOfWindows;
    private static double diff;
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.get("input", pathToTaskEventData);

        length = params.getLong("length");
        slide = params.getLong("slide");
        numberOfWindows = params.getInt("windows");
        diff = params.getDouble("diff");

        // events of 10 minute are served in 1 second
        final int servingSpeedFactor = 60000;

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<TaskEvent> taskEvents = env
                .addSource(taskSourceOrTest(new TaskEventSource(input, servingSpeedFactor)))
                .setParallelism(1);

        DataStream<Tuple3<Long, Double, Double>> res = taskEvents
                .filter(new MyFilterFunction())
                .keyBy((TaskEvent t) -> t.machineId)
                .timeWindow(Time.milliseconds(length), Time.milliseconds(slide))
                .apply(new MyWindowFunction())
                .flatMap(new MyFlatMapFunction())
                .keyBy(0)
                .process(new MyKeyProcessFunction());

        printOrTest(res);
        env.execute("Time-Series Anomaly Test");
    }

    public static class MyFilterFunction implements FilterFunction<TaskEvent> {

        @Override
        public boolean filter(TaskEvent taskEvent) throws Exception {
            return taskEvent.eventType.getValue() == 1 && taskEvent.maxCPU > 0;
        }
    }

    public static class MyWindowFunction implements WindowFunction<TaskEvent, TimeTask, Long, TimeWindow> {

        @Override
        public void apply(Long key, TimeWindow window, Iterable<TaskEvent> iterable, Collector<TimeTask> out) throws Exception {
            int count = 0;
            // double here or the sum will keep being 0
            double sum = 0;
            for (TaskEvent t : iterable) {
                sum += t.maxCPU;
                count++;
            }
            double curAvg = sum / (double) count;
            if (count > 0) {
                out.collect(TimeTask.of(key, curAvg, window.getEnd()));
            }
        }
    }

    public static class MyFlatMapFunction implements FlatMapFunction<TimeTask, Tuple2<Long, TimeTask>> {

        @Override
        public void flatMap(TimeTask timeTask, Collector<Tuple2<Long, TimeTask>> out) throws Exception {
            out.collect(new Tuple2<>(timeTask.key, timeTask));
        }
    }

    public static class MyKeyProcessFunction extends KeyedProcessFunction<Tuple, Tuple2<Long, TimeTask>, Tuple3<Long, Double, Double>> {
        // key: window end
        private MapState<Long, TimeTask> movingTimeTask;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            MapStateDescriptor<Long, TimeTask> items = new MapStateDescriptor<>(
                    "movingTimeTask", Long.class, TimeTask.class);
            movingTimeTask = getRuntimeContext().getMapState(items);
        }

        @Override
        public void processElement(Tuple2<Long, TimeTask> in, Context ctx, Collector<Tuple3<Long, Double, Double>> out) throws Exception {
            // keep put window's end and object into mapstate
            long end = in.f1.timestamp;
            movingTimeTask.put(end, in.f1);
            ctx.timerService().registerEventTimeTimer(end + (numberOfWindows  - 1) * slide);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<Long, Double, Double>> out) throws Exception {
            List<TimeTask> all = new ArrayList<>();
            boolean triggered = true;
            for (int i = 0; i < numberOfWindows; i++) {
                if (!movingTimeTask.contains(timestamp - i * slide)) triggered = false;
            }
            if (triggered) {
                TimeTask cur = movingTimeTask.get(timestamp);
                for (int i = 0; i < numberOfWindows; i++) {
                    TimeTask t = movingTimeTask.get(timestamp - i * slide);
                    all.add(t);
                }

                double sum = 0;
                for (TimeTask t : all) {
                    sum += t.curAvg;
                }
                double moveAvg = sum / (double) all.size();
                double curAvg = cur.curAvg;
                double val = 2 * Math.abs(curAvg - moveAvg) / (curAvg + moveAvg);
                if (val > diff) {
                    out.collect(new Tuple3<>(cur.key, curAvg, moveAvg));
                }
            }
        }
    }
}
