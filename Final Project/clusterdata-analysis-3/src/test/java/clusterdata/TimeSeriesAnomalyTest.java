package clusterdata;

import clusterdata.datatypes.EventType;
import clusterdata.datatypes.TaskEvent;
import clusterdata.exercise3.TimeSeriesAnomaly;
import clusterdata.testing.TaskEventTestBase;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Test;

import java.util.List;
import static org.junit.Assert.assertEquals;

public class TimeSeriesAnomalyTest extends TaskEventTestBase<TaskEvent> {
    static Testable javaExercise = () -> TimeSeriesAnomaly.main(new String[] {"--length", "5000", "--slide", "2000", "--windows", "3", "--diff", "0.3"});

    @Test
    public void testTimeSeriesAnomaly() throws Exception {
        // machine 1 till 13
        TaskEvent a1 = testEvent(1, 5.0, 1000);
        TaskEvent a2 = testEvent(1, 2.0, 3000);
        TaskEvent a3 = testEvent(1, 8.0, 4900);
        TaskEvent a4 = testEvent(1, 12.0, 8900);
        TaskEvent a5 = testEvent(1, 1.0, 12900);

        // machine 3
//        TaskEvent c1 = testEvent(3, 3.0, 7000);
//        TaskEvent c2 = testEvent(3, 4.0, 8000);
//        TaskEvent c3 = testEvent(3, 8.0, 11100);
//        TaskEvent c4 = testEvent(3, 9.0, 12000);

        TestTaskEventSource source = new TestTaskEventSource(
//                c1, c2, c3, c4
                a1, a2, a3, a4, a5
        );

        assertEquals(Lists.newArrayList(
                new Tuple3<Long, Double, Double>(1L, 10.0, 6.67),
                new Tuple3<Long, Double, Double>(1L, 6.5, 9.5),
                new Tuple3<Long, Double, Double>(1L, 1.0, 6.5),
                new Tuple3<Long, Double, Double>(1L, 1.0, 2.83)
//                new Tuple3<Long, Double, Double>(3L, 7.0, 4.67)
    ), results(source));
    }

    private TaskEvent testEvent(long machineId, double maxCPU, long timestamp) {
        return new TaskEvent(42, 9, timestamp, machineId, EventType.SCHEDULE, "Kate",
                2, 1, maxCPU, 0d, 0d, false, "123");
    }

    protected List<?> results(TestTaskEventSource source) throws Exception {
        return runApp(source, new TestSink<>(), javaExercise);
    }
}
