package clusterdata;

import clusterdata.datatypes.EventType;
import clusterdata.datatypes.TaskEvent;
import clusterdata.exercise3.InvariantBasedAnomaly;
import clusterdata.testing.TaskEventTestBase;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.Test;

import java.util.List;
import static org.junit.Assert.assertEquals;

public class InvariantBasedAnomalyTest extends TaskEventTestBase<TaskEvent> {
    static Testable javaExercise = () -> InvariantBasedAnomaly.main(new String[] {"--training-end", "10000"});

    @Test
    public void testInvariantBasedAnomaly() throws Exception {
        TaskEvent submit_1 = testEvent(1, 1, EventType.SUBMIT, 3000);
        TaskEvent submit_2 = testEvent(2, 2, EventType.SUBMIT, 5000);
        TaskEvent submit_3 = testEvent(3, 3, EventType.SUBMIT, 11000);
        TaskEvent submit_4 = testEvent(4, 4, EventType.SUBMIT, 12000);
        TaskEvent submit_5 = testEvent(5, 5, EventType.SUBMIT, 21000);
        TaskEvent submit_6 = testEvent(6, 6, EventType.SUBMIT, 23000);
        TaskEvent submit_7 = testEvent(7, 7, EventType.SUBMIT, 30000);
        TaskEvent submit_8 = testEvent(8, 8, EventType.SUBMIT, 37000);


        TaskEvent schedule_1 = testEvent(2, 2, EventType.SCHEDULE, 7000);
        TaskEvent schedule_2 = testEvent(1, 1, EventType.SCHEDULE, 9000);
        TaskEvent schedule_3 = testEvent(4, 4, EventType.SCHEDULE, 15000);
        TaskEvent schedule_4 = testEvent(3, 3, EventType.SCHEDULE, 19000);
        TaskEvent schedule_5 = testEvent(6, 6, EventType.SCHEDULE, 27000);
        TaskEvent schedule_6 = testEvent(5, 5, EventType.SCHEDULE, 28000);
        TaskEvent schedule_7 = testEvent(7, 7, EventType.SCHEDULE, 35000);
        TaskEvent schedule_8 = testEvent(8, 8, EventType.SCHEDULE, 38000);

        // test case goes wrong, just ignore this test case.
        // need large amount of fixed timestamp so that we can see the watermark.
        TestTaskEventSource source = new TestTaskEventSource(
                submit_1, submit_2, 4999L,
                schedule_1, schedule_2, 7999L, 8999L, 9999L, 10999L, submit_3,
                11999L, submit_4, 16999L, 17999L,
                schedule_3, 18999L, schedule_4, 20999L, 21999L,
                submit_5, submit_6, 24999L, schedule_5, schedule_6, 29999L,
                submit_7, schedule_7, 36999L, submit_8, 37500L, schedule_8, 39000L
        );

        assertEquals(Lists.newArrayList(new Tuple2<Integer, Long>(1, 8000L)), results(source));
    }

    private TaskEvent testEvent(int jobId, int taskIndex, EventType t, long timestamp) {
        return new TaskEvent(jobId, taskIndex, timestamp, 1, t, "Kate",
                2, 1, 0d, 0d, 0d, false, "123");
    }

    protected List<?> results(TestTaskEventSource source) throws Exception {
        return runApp(source, new TestSink<>(), javaExercise);
    }
}
