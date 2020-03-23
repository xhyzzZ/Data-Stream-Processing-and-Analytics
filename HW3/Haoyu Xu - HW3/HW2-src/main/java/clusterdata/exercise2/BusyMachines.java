package clusterdata.exercise2;

import clusterdata.datatypes.TaskEvent;
import clusterdata.sources.TaskEventSource;
import clusterdata.utils.AppBase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Write a program that identifies every 5 minutes the busiest machines in the cluster during the last 15 minutes.
 * For every window period, the program must then output the number of busy machines during that last period.
 *
 * A machine is considered busy if more than a threshold number of tasks have been scheduled on it
 * during the specified window period (size).
 */

class JobBusyMachine {
    public long windowEnd;  // window end timestamp
    public long busyCount;  // number of busy machine
    public long key;

    public static JobBusyMachine of(long key, long windowEnd, long busyCount) {
        JobBusyMachine result = new JobBusyMachine();
        result.key = key;
        result.windowEnd = windowEnd;
        result.busyCount = busyCount;
        return result;
    }
}
public class BusyMachines extends AppBase {
    private static int threshold;
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.get("input", pathToTaskEventData);
        final int busyThreshold = params.getInt("threshold", 15);
        threshold = busyThreshold;
        final int servingSpeedFactor = 60000; // events of 10 minute are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        // set latency tracking every 500ms
//        env.getConfig().setLatencyTrackingInterval(500);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        DataStream<TaskEvent> taskEvents = env
                .addSource(taskSourceOrTest(new TaskEventSource(input, servingSpeedFactor)))
                .setParallelism(1);
        //TODO: implement the program logic here
        //DataStream<Tuple2<Long, Integer>> busyMachinesPerWindow = ...
        //printOrTest(busyMachinesPerWindow);
        // fail one out of ten tests because of some reasons
        DataStream<Tuple2<Long, Integer>> busyMachinesPerWindow = taskEvents
                .filter(new MyFilterFunction())
                .keyBy((TaskEvent t) -> t.machineId)
                .timeWindow(Time.minutes(15), Time.minutes(5))
                .apply(new MyWindowFunction())
                .flatMap(new MyFlatMapFunction())
                .keyBy(0)
                .process(new MyProcessFunction());

//        busyMachinesPerWindow.addSink(new SinkFunction<Tuple2<Long, Integer>>() {
//            @Override
//            public void invoke(Tuple2<Long, Integer> value, Context context) throws Exception {
//                // no-op sink
//            }
//        });

        printOrTest(busyMachinesPerWindow);
        env.execute("Number of busy machines every 5 minutes over the last 15 minutes");
    }


    public static class MyFilterFunction implements FilterFunction<TaskEvent> {

        @Override
        public boolean filter(TaskEvent taskEvent) throws Exception {
            return taskEvent.eventType.getValue() == 1;
        }
    }

    public static class MyWindowFunction implements WindowFunction<TaskEvent, JobBusyMachine, Long, TimeWindow> {

        @Override
        public void apply(Long key, TimeWindow window, Iterable<TaskEvent> in, Collector<JobBusyMachine> out) throws Exception {
            int count = 0;
            for (TaskEvent t : in) {
                count++;
            }
            out.collect(JobBusyMachine.of(key, window.getEnd(), count));
        }
    }

    public static class MyFlatMapFunction implements FlatMapFunction<JobBusyMachine, Tuple2<Long, JobBusyMachine>> {

        @Override
        public void flatMap(JobBusyMachine in, Collector<Tuple2<Long, JobBusyMachine>> out) throws Exception {
            out.collect(new Tuple2<>(in.windowEnd, in));
        }
    }

    public static class MyProcessFunction extends KeyedProcessFunction<Tuple, Tuple2<Long, JobBusyMachine>, Tuple2<Long, Integer>> {
        private ListState<JobBusyMachine> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor<JobBusyMachine> itemsStateDesc = new ListStateDescriptor<>(
                    "state", JobBusyMachine.class);
            state = getRuntimeContext().getListState(itemsStateDesc);
        }
        @Override
        public void processElement(Tuple2<Long, JobBusyMachine> in, Context ctx, Collector<Tuple2<Long, Integer>> out) throws Exception {
            state.add(in.f1);
            ctx.timerService().registerEventTimeTimer(in.f1.windowEnd + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Long, Integer>> out) throws Exception {
            List<JobBusyMachine> allItems = new ArrayList<>();
            for (JobBusyMachine item : state.get()) {
                allItems.add(item);
            }
            // clear state data
            state.clear();
            int count = 0;
            for (JobBusyMachine j : allItems) {
                if (j.busyCount >= threshold) {
                    count++;
                }
            }
            if (count != 0) {
                out.collect(new Tuple2<>(timestamp - 1, count));
            }
        }
    }
}
