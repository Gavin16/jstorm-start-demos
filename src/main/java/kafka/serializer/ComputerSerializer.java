package kafka.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class ComputerSerializer implements Serializer {

    private ObjectMapper objectMapper;

    @Override
    public void configure(Map map, boolean b) {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(String s, Object o) {
        byte[] bytes = null;
        try {
            bytes = objectMapper.writeValueAsString(o).getBytes("utf-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bytes;
    }

    @Override
    public void close() {

    }
}
