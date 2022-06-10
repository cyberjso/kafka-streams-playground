package io.joliveira;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper mapper;

    private final Class<T> clazz;

    public JsonDeserializer(ObjectMapper mapper, Class<T> clazz) {
        this.mapper = mapper;
        this.clazz = clazz;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null)
            return null;

        try {
            return (T) mapper.readValue(data, clazz);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing message", e);
        }
    }

}
