package com.travelsky.dataplatform.utils;



import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.function.Function;

/**
 * 通用 Doris 序列化工具类
 * 支持任意类型的对象序列化到 Doris，默认过滤所有空字符串字段
 */
public class GenericDorisSerializer<T> implements DorisRecordSerializer<T> {

    // 默认的 ObjectMapper (线程安全)
    private static final ObjectMapper DEFAULT_MAPPER = createDefaultMapper();

    // 使用的 ObjectMapper 实例
    private final ObjectMapper objectMapper;

    // 自定义转换函数
    private final Function<T, T> transformFunction;

    // 是否在序列化失败时返回空记录
    private final boolean returnEmptyOnError;

    /**
     * 私有构造方法
     */
    private GenericDorisSerializer(ObjectMapper mapper, Function<T, T> transform, boolean returnEmptyOnError) {
        this.objectMapper = mapper != null ? mapper : DEFAULT_MAPPER;
        this.transformFunction = transform;
        this.returnEmptyOnError = returnEmptyOnError;
    }

    /**
     * 创建默认的 ObjectMapper
     */
    private static ObjectMapper createDefaultMapper() {
        ObjectMapper mapper = new ObjectMapper();
        // 禁用日期时间戳格式
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }
    
    /**
     * 创建支持大写下划线命名的 ObjectMapper
     * 用于Doris表列名映射(驼峰转大写下划线)
     * 例如: orderNumber -> ORDER_NUMBER
     * 使用旧版Jackson API兼容
     */
    private static ObjectMapper createSnakeCaseMapper() {
        ObjectMapper mapper = new ObjectMapper();
        // 禁用日期时间戳格式
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        // 设置属性命名策略为大写下划线(兼容旧版本Jackson)
        mapper.setPropertyNamingStrategy(new PropertyNamingStrategy.PropertyNamingStrategyBase() {
            @Override
            public String translate(String propertyName) {
                if (propertyName == null || propertyName.isEmpty()) {
                    return propertyName;
                }
                // 驼峰转大写下划线: orderNumber -> ORDER_NUMBER
                StringBuilder result = new StringBuilder();
                result.append(Character.toUpperCase(propertyName.charAt(0)));
                for (int i = 1; i < propertyName.length(); i++) {
                    char c = propertyName.charAt(i);
                    if (Character.isUpperCase(c)) {
                        result.append('_').append(c);
                    } else {
                        result.append(Character.toUpperCase(c));
                    }
                }
                return result.toString();
            }
        });
        return mapper;
    }

    /**
     * 核心：递归过滤对象中的空字符串字段
     * @param node JSON节点（支持嵌套对象）
     */
    private void filterEmptyValues(ObjectNode node) {
        if (node == null) {
            return;
        }
        // 迭代所有字段，移除空字符串值
        Iterator<String> fieldNames = node.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            // 1. 过滤Null值字段
            if (node.get(fieldName).isNull()) {
                fieldNames.remove();
            }
            // 2. 过滤空字符串/纯空格字符串
            else if (node.get(fieldName).isTextual()) {
                String textValue = node.get(fieldName).asText().trim();
                if (textValue.isEmpty()) {
                    fieldNames.remove();
                } else if ("null".equalsIgnoreCase(textValue)) {
                    fieldNames.remove();
                }
            }
            // 3. 递归处理嵌套对象，确保深层字段也被过滤
            else if (node.get(fieldName).isObject()) {
                filterEmptyValues((ObjectNode) node.get(fieldName));
            }
        }
    }

    @Override
    public DorisRecord serialize(T record) {
        try {
            // 应用转换函数（如果存在）
            T dataToSerialize = record;
            if (transformFunction != null) {
                dataToSerialize = transformFunction.apply(record);
            }

            // 步骤1：将对象转为ObjectNode（便于修改字段）
            ObjectNode jsonNode = objectMapper.valueToTree(dataToSerialize);

            // 步骤2：强制过滤空字符串字段（核心修改点）
            filterEmptyValues(jsonNode);

            // 步骤3：序列化为 JSON 字符串
            String json = objectMapper.writeValueAsString(jsonNode);
//            System.out.println("Record json:"+json);

            // 创建 Doris 记录
            return DorisRecord.of(json.getBytes(StandardCharsets.UTF_8));
        } catch (JsonProcessingException e) {
            // 错误处理策略
            if (returnEmptyOnError) {
                return DorisRecord.of(new byte[0]);
            } else {
                throw new SerializationException("Serialization error for record: " + record, e);
            }
        }
    }

    /**
     * 自定义序列化异常
     */
    public static class SerializationException extends RuntimeException {
        public SerializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    // ================ Builder 模式 ================ //

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T> {
        private ObjectMapper objectMapper;
        private Function<T, T> transformFunction;
        private boolean returnEmptyOnError = false;

        public Builder<T> objectMapper(ObjectMapper mapper) {
            this.objectMapper = mapper;
            return this;
        }

        public Builder<T> transform(Function<T, T> transform) {
            this.transformFunction = transform;
            return this;
        }

        public Builder<T> returnEmptyOnError(boolean returnEmpty) {
            this.returnEmptyOnError = returnEmpty;
            return this;
        }

        public GenericDorisSerializer<T> build() {
            return new GenericDorisSerializer<>(objectMapper, transformFunction, returnEmptyOnError);
        }
    }

    // ================ 快捷创建方法 ================ //

    /**
     * 创建基本序列化器
     */
    public static <T> GenericDorisSerializer<T> create() {
        return new GenericDorisSerializer<>(null, null, false);
    }

    /**
     * 创建带日期格式化的序列化器
     */
    public static <T> GenericDorisSerializer<T> createWithDateFormat(String pattern) {
        ObjectMapper customMapper = DEFAULT_MAPPER.copy();
        customMapper.setDateFormat(new java.text.SimpleDateFormat(pattern));
        return new GenericDorisSerializer<>(customMapper, null, false);
    }

    /**
     * 创建带字段转换的序列化器
     */
    public static <T> GenericDorisSerializer<T> createWithTransform(Function<T, T> transform) {
        return new GenericDorisSerializer<>(null, transform, false);
    }

    /**
     * 创建安全序列化器（错误时返回空）
     */
    public static <T> GenericDorisSerializer<T> createSafe() {
        return new GenericDorisSerializer<>(null, null, true);
    }
    
    /**
     * 创建支持大写下划线命名的序列化器（用于Doris）
     * 将Java驼峰命名自动转换为Doris大写下划线命名
     * 例如: orderNumber -> ORDER_NUMBER
     */
    public static <T> GenericDorisSerializer<T> createForDoris() {
        return new GenericDorisSerializer<>(createSnakeCaseMapper(), null, true);
    }
}
