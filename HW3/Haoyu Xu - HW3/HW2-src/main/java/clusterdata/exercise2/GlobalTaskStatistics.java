package clusterdata.exercise2;

import clusterdata.datatypes.TaskEvent;
import clusterdata.sources.TaskEventSource;
import clusterdata.utils.AppBase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Count successful, failed, and killed tasks per minute.
 */
public class GlobalTaskStatistics extends AppBase {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.get("input", pathToTaskEventData);

        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);

        // start the data generator
        DataStream<TaskEvent> taskEvents = env
                .addSource(taskSourceOrTest(new TaskEventSource(input, servingSpeedFactor)))
                .setParallelism(1);

        //TODO: implement the window logic here
        // DataStream<Tuple4<Long, Integer, Integer, Integer>> statistics =
        // printOrTest(statistics);

        DataStream<TaskEvent> filteredTasks = taskEvents.filter(new MyFilterFunction());

        DataStream<Tuple4<Long, Integer, Integer, Integer>> statistics = filteredTasks
                .timeWindowAll(Time.minutes(1))
                .apply(new MyAllWindowFunction());

        printOrTest(statistics);
        env.execute("Global task statistics");
    }

    public static class MyFilterFunction implements FilterFunction<TaskEvent> {

        @Override
        public boolean filter(TaskEvent taskEvent) throws Exception {
            return taskEvent.eventType.getValue() == 3
                    || taskEvent.eventType.getValue() == 4
                    || taskEvent.eventType.getValue() == 5;
        }
    }

    public static class MyAllWindowFunction implements AllWindowFunction<TaskEvent, Tuple4<Long, Integer, Integer, Integer>, TimeWindow> {

        @Override
        public void apply(TimeWindow window, Iterable<TaskEvent> iterable, Collector<Tuple4<Long, Integer, Integer, Integer>> out) throws Exception {
            int succ = 0, fail = 0, kill = 0;
            for (TaskEvent t : iterable) {
                if (t.eventType.getValue() == 3) {
                    fail++;
                } else if (t.eventType.getValue() == 4) {
                    succ++;
                } else {
                    kill++;
                }
            }
            out.collect(new Tuple4<>(window.getStart(), succ, fail, kill));
        }
    }
}
