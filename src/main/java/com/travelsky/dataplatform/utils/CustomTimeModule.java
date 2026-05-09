package com.travelsky.dataplatform.utils;

//import com.fasterxml.jackson.core.JsonGenerator;
//import com.fasterxml.jackson.databind.JsonSerializer;
//import com.fasterxml.jackson.databind.SerializerProvider;
//import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.doris.shaded.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.doris.shaded.com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.doris.shaded.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.doris.shaded.com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;

/**
 * 自定义时间的序列化器
 */
public class CustomTimeModule extends SimpleModule {
    private static String pattern;


    public CustomTimeModule(String pattern) {
        this.pattern =pattern;
    }


    public CustomTimeModule() {
        addSerializer(Timestamp.class, new LocalDateTimeSerializer());
        // 可以添加其他时间类型的序列化器
    }

    public static class LocalDateTimeSerializer extends JsonSerializer<Timestamp> implements Serializable {




        @Override
        public void serialize(Timestamp timestamp, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
            final DateTimeFormatter formatter =
                    DateTimeFormatter.ofPattern(pattern);
            jsonGenerator.writeString(timestamp.toLocalDateTime().format(formatter));
        }
    }
}