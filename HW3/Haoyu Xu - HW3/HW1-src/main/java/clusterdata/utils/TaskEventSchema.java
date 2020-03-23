package clusterdata.utils;

import clusterdata.datatypes.EventType;
import clusterdata.datatypes.TaskEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Implements a SerializationSchema and DeserializationSchema for TaskEvent for Kafka data sources and sinks.
 */
public class TaskEventSchema implements DeserializationSchema<TaskEvent>, SerializationSchema<TaskEvent>  {
    private ObjectMapper objectMapper;
    @Override
    public byte[] serialize(TaskEvent element) {
        //TODO: implement this method
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }

        try {
            return objectMapper.writeValueAsString(element).getBytes();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public TaskEvent deserialize(byte[] message) throws IOException {
        //TODO: implement this method
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }

        return objectMapper.readValue(message, TaskEvent.class);
    }

    @Override
    public boolean isEndOfStream(TaskEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<TaskEvent> getProducedType() {
        return TypeExtractor.getForClass(TaskEvent.class);
    }
}
