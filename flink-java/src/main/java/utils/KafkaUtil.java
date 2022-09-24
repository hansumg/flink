package utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaUtil {

    public static <T> FlinkKafkaConsumer<T> getConsumer(String topic , String groupid,
                                                        DeserializationSchema deserializationSchema, Properties prop) {
        return new FlinkKafkaConsumer<T>(
                topic,
                deserializationSchema,
                prop

        );
    }
}
