package com.travelsky.dataplatform.utils;

import org.apache.doris.flink.deserialization.DorisDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;

/**
 * 通用JSON反序列化器，支持自动映射到POJO类
 * @param <T> 目标类型
 */
public class GenericJsonDeserializer<T> implements DorisDeserializationSchema<T> {

    private final Class<T> targetClass;
    private final String[] fieldNames;
    private transient ObjectMapper objectMapper;

    /**
     * @param targetClass 目标POJO类
     * @param fieldNames 字段名称数组，与Doris查询结果的列顺序对应
     */
    public GenericJsonDeserializer(Class<T> targetClass, String[] fieldNames) {
        this.targetClass = targetClass;
        this.fieldNames = fieldNames;
    }

    @Override
    public void deserialize(List<?> row, Collector<T> collector) throws Exception {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }

        try {
            // 将行数据转换为JSON对象
            ObjectNode node = objectMapper.createObjectNode();
            for (int i = 0; i < Math.min(row.size(), fieldNames.length); i++) {
                Object value = row.get(i);
                if (value != null) {
                    if (value instanceof Number) {
                        node.put(fieldNames[i], (Long) value);
                    } else if (value instanceof Boolean) {
                        node.put(fieldNames[i], (Boolean) value);
                    } else {
                        node.put(fieldNames[i], value.toString());
                    }
                }
            }

            // 将JSON转换为目标对象
            T result = objectMapper.treeToValue(node, targetClass);
            collector.collect(result);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize row: " + row, e);
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(targetClass);
    }
}