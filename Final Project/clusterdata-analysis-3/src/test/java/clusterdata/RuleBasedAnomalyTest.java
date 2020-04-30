package clusterdata;

import clusterdata.datatypes.EventType;
import clusterdata.datatypes.TaskEvent;
import clusterdata.exercise3.RuleBasedAnomaly;
import clusterdata.testing.TaskEventTestBase;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Test;

import java.util.List;
import static org.junit.Assert.assertEquals;

public class RuleBasedAnomalyTest extends TaskEventTestBase<TaskEvent> {
    static Testable javaExercise = () -> RuleBasedAnomaly.main(new String[]{"--e1", "0", "--e2", "1", "--e3", "5", "--time-constraint", "10000"});

    @Test
    public void testRuleBasedAnomaly() throws Exception {
        TaskEvent submit_1 = testEvent(1, 1, EventType.SUBMIT, 5000);
        TaskEvent submit_2 = testEvent(2, 2, EventType.SUBMIT, 7000);
        TaskEvent submit_3 = testEvent(3, 3, EventType.SUBMIT, 8000);

        TaskEvent schedule_1 = testEvent(2, 2, EventType.SCHEDULE, 9000);
        TaskEvent schedule_2 = testEvent(1, 1, EventType.SCHEDULE, 10000);
        TaskEvent schedule_3 = testEvent(3, 3, EventType.SCHEDULE, 13000);

        TaskEvent kill_1 = testEvent(2, 2, EventType.KILL, 12000);
        TaskEvent kill_2 = testEvent(1, 1, EventType.KILL, 17000);

        TaskEvent finish_1 = testEvent(1, 1, EventType.FINISH, 14000);

        TestTaskEventSource source = new TestTaskEventSource(
                submit_1, submit_2, submit_3,
                schedule_1, schedule_2, kill_1, schedule_3, finish_1,
                kill_2
        );

        assertEquals(Lists.newArrayList(new Tuple3<TaskEvent, TaskEvent, TaskEvent>(submit_2, schedule_1, kill_1)), results(source));
    }

    private TaskEvent testEvent(long jobId, int taskIndex, EventType t, long timestamp) {
        return new TaskEvent(jobId, taskIndex, timestamp, 1, t, "Kate",
                2, 1, 0d, 0d, 0d, false, "123");
    }

    protected List<?> results(TestTaskEventSource source) throws Exception {
        return runApp(source, new TestSink<>(), javaExercise);
    }
}
