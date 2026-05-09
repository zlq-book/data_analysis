package com.travelsky.trp.usercenter.data.analysis.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingSegFactModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 自定义JSON序列化工具类，用于处理带有注解冲突的对象
 * 支持 LocalDate/LocalDateTime 类型
 */
public class CustomJsonSerializer {

    private static final Logger logger = LoggerFactory.getLogger(CustomJsonSerializer.class);

    private static final ObjectMapper objectMapper = createObjectMapper();

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();

        // 1. 配置ObjectMapper只使用JsonProperty注解，忽略getter/setter命名不一致的问题
        mapper.setAnnotationIntrospector(new JacksonAnnotationIntrospector() {
            @Override
            public boolean hasIgnoreMarker(AnnotatedMember m) {
                // 忽略没有JsonProperty注解的字段
                JsonProperty property = _findAnnotation(m, JsonProperty.class);
                return property == null;
            }
        });

        // 2. 只序列化非null值（注意：NON_EMPTY 会过滤空字符串，建议用 NON_NULL）
        // objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

        // 3. 配置不序列化空 beans
        mapper.configure(com.fasterxml.jackson.databind.SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

        // 4. 添加自定义模块处理 Java 8 时间类型
        addJava8TimeSupport(mapper);

        return mapper;
    }

    /**
     * 为 Jackson 添加 Java 8 时间类型支持
     */
    private static void addJava8TimeSupport(ObjectMapper mapper) {
        SimpleModule module = new SimpleModule("Java8TimeModule");

        // LocalDate 序列化器
        module.addSerializer(LocalDate.class, new JsonSerializer<LocalDate>() {
            private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

            @Override
            public void serialize(LocalDate value, JsonGenerator gen, SerializerProvider serializers)
                    throws IOException {
                if (value == null) {
                    gen.writeNull();
                } else {
                    gen.writeString(value.format(formatter));
                }
            }

            @Override
            public Class<LocalDate> handledType() {
                return LocalDate.class;
            }
        });

        // LocalDateTime 序列化器（如果需要）
        module.addSerializer(LocalDateTime.class, new JsonSerializer<LocalDateTime>() {
            private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            @Override
            public void serialize(LocalDateTime value, JsonGenerator gen, SerializerProvider serializers)
                    throws IOException {
                if (value == null) {
                    gen.writeNull();
                } else {
                    gen.writeString(value.format(formatter));
                }
            }

            @Override
            public Class<LocalDateTime> handledType() {
                return LocalDateTime.class;
            }
        });

        // 如果需要，可以添加反序列化器
        // module.addDeserializer(LocalDate.class, new LocalDateDeserializer());
        // module.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer());

        mapper.registerModule(module);
    }

    /**
     * 将对象转换为JSON字符串，只序列化带有@JsonProperty注解的字段
     *
     * @param object 要序列化的对象
     * @return JSON字符串
     */
    public static String toJsonWithAnnotationsOnly(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize object to JSON: {}", e.getMessage(), e);
            return "{}";
        }
    }

    /**
     * 测试方法：验证 LocalDate 序列化是否正常
     */
    public static void main(String[] args) {
        try {

            TickingSegFactModel testObj = new TickingSegFactModel();
            testObj.setPkId("1111");
            testObj.setFkPassengerUserTid("");

            // 序列化
            String json = toJsonWithAnnotationsOnly(testObj);
            logger.info("Test serialization result: {}", json);



        } catch (Exception e) {
            logger.error("LocalDate serialization test FAILED with error: {}", e.getMessage());
        }
    }

}