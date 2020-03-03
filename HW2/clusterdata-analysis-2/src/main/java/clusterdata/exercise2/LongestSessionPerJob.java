package clusterdata.exercise2;

import clusterdata.datatypes.JobEvent;
import clusterdata.datatypes.TaskEvent;
import clusterdata.sources.JobEventSource;
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
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Write a program that identifies job stages by their sessions.
 * We assume that a job will execute its tasks in stages and that a stage has finished after an inactivity period of 10 minutes.
 * The program must output the longest session per job, after the job has finished.
 */

class SessionJob {
    public long windowEnd;  // window end timestamp
    public int jobCount;  // number of busy machine
    public long key;

    public static SessionJob of(long key, long windowEnd, int jobCount) {
        SessionJob result = new SessionJob();
        result.key = key;
        result.windowEnd = windowEnd;
        result.jobCount = jobCount;
        return result;
    }
}
public class LongestSessionPerJob extends AppBase {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String taskInput = params.get("task_input", pathToTaskEventData);
        String jobInput = params.get("job_input", pathToJobEventData);

        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        DataStream<TaskEvent> taskEvents = env
                .addSource(taskSourceOrTest(new TaskEventSource(taskInput, servingSpeedFactor)))
                .setParallelism(1);

        DataStream<JobEvent> jobEvents = env
                .addSource(jobSourceOrTest(new JobEventSource(jobInput, servingSpeedFactor)))
                .setParallelism(1);
        DataStream<JobEvent> keyJob = jobEvents.keyBy("jobId");
        //TODO: implement the program logic here

        //DataStream<Tuple2<Long, Integer>> maxSessionPerJob = ...
        //printOrTest(maxSessionPerJob);
        // fail one out of ten tests because of some reasons
        DataStream<Tuple2<Long, Integer>> maxSessionPerJob = taskEvents
                .filter(new MyFilterFunction())
                .keyBy((TaskEvent t) -> t.jobId)
                .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
                .apply(new MyWindowSession())
                .flatMap(new MyFlatMapFunction())
                .keyBy(0)
                .connect(keyJob)
                .process(new KeyedCoProcessFunction<Tuple, Tuple2<Long, SessionJob>, JobEvent, Tuple2<Long, Integer>>() {
                    private ListState<SessionJob> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ListStateDescriptor<SessionJob> itemsStateDesc = new ListStateDescriptor<>(
                                "state", SessionJob.class);
                        state = getRuntimeContext().getListState(itemsStateDesc);
                    }

                    @Override
                    public void processElement1(Tuple2<Long, SessionJob> in, Context ctx, Collector<Tuple2<Long, Integer>> out) throws Exception {
                        state.add(in.f1);
                    }

                    @Override
                    public void processElement2(JobEvent in, Context ctx, Collector<Tuple2<Long, Integer>> out) throws Exception {
                        if (in.eventType.getValue() == 4) {
                            ctx.timerService().registerEventTimeTimer(in.timestamp + 100);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Long, Integer>> out) throws Exception {
                        List<SessionJob> allItems = new ArrayList<>();
                        for (SessionJob item : state.get()) {
                            allItems.add(item);
                        }
                        // clear state data
                        state.clear();
                        int max = -1;
                        SessionJob s = new SessionJob();
                        for (SessionJob j : allItems) {
                            if (max < j.jobCount) {
                                s = j;
                                max = j.jobCount;
                            }
                        }
                        out.collect(new Tuple2<>(s.key, max));
                    }
                });
        
        printOrTest(maxSessionPerJob);
        env.execute("Maximum stage size per job");
    }

    public static class MyFilterFunction implements FilterFunction<TaskEvent> {

        @Override
        public boolean filter(TaskEvent taskEvent) throws Exception {
            return taskEvent.eventType.getValue() == 0;
        }
    }

    public static class MyWindowSession implements WindowFunction<TaskEvent, SessionJob, Long, TimeWindow> {

        @Override
        public void apply(Long key, TimeWindow window, Iterable<TaskEvent> in, Collector<SessionJob> out) throws Exception {
            int count = 0;
            for (TaskEvent t : in) {
                count++;
            }
            out.collect(SessionJob.of(key, window.getEnd(), count));
        }
    }

    public static class MyFlatMapFunction implements FlatMapFunction<SessionJob, Tuple2<Long, SessionJob>> {

        @Override
        public void flatMap(SessionJob in, Collector<Tuple2<Long, SessionJob>> out) throws Exception {
            out.collect(new Tuple2<>(in.key, in));
        }
    }

}
