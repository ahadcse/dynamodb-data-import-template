package com.awsdynamodb.importer.client;

import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.awsdynamodb.importer.model.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

@Service
public class DynamoDBClient {

    private final DynamoDBMapper dynamoDBMapper;
    private final UnaryOperator<String> cloudFormationService;

    @Autowired
    public DynamoDBClient(DynamoDBMapper dynamoDBMapper, UnaryOperator<String> cloudFormationService) {
        this.dynamoDBMapper = dynamoDBMapper;
        this.cloudFormationService = cloudFormationService;
    }

    public void insert(Object obj) {
        dynamoDBMapper.save(obj, buildDynamoDBMapperConfig(obj.getClass()));
    }

    public List<DynamoDBMapper.FailedBatch> batchInsertData(List<Data> datas) {
        return dynamoDBMapper.batchSave(datas);
    }


    public void delete(Long hashKey, Class objClazz) {
        DynamoDBMapperConfig config = buildDynamoDBMapperConfig(objClazz);
        Object load = dynamoDBMapper.load(objClazz, hashKey, config);
        if (load == null) {
            return;
        }
        dynamoDBMapper.delete(load, config);
    }

    public <T> T getData(Long hashKey, Class<T> objClazz) {
        DynamoDBMapperConfig config = buildDynamoDBMapperConfig(objClazz);
        DynamoDBQueryExpression<T> queryExpression = buildQueryExpression(hashKey, objClazz);
        List<T> publisherDatas = dynamoDBMapper.query(objClazz, queryExpression, config);
        if (publisherDatas.isEmpty()) {
            return null;
        }
        if (publisherDatas.size() > 1) {
            throw new IllegalStateException("Found too many rows for hashKey " + hashKey);
        }
        return publisherDatas.get(0);
    }

    private DynamoDBMapperConfig buildDynamoDBMapperConfig(Class objClazz) {
        String tableName = getTableName(objClazz);
        return new DynamoDBMapperConfig.Builder()
                .withTableNameOverride(new DynamoDBMapperConfig.TableNameOverride(tableName))
                .build();
    }

    private String getTableName(Class objClazz) {
        if (objClazz.isAnnotationPresent(DynamoDBTable.class)) {
            Annotation annotation = objClazz.getAnnotation(DynamoDBTable.class);
            DynamoDBTable dynamoDBTable = (DynamoDBTable) annotation;
            String logicalName = dynamoDBTable.tableName();
            return cloudFormationService.apply(logicalName);
        }
        return null;
    }

    private <T> DynamoDBQueryExpression<T> buildQueryExpression(Long hashKey, Class<T> objClazz) {
        return new DynamoDBQueryExpression<T>()
                .withConsistentRead(false)
                .withKeyConditionExpression(buildConditionExpression(objClazz))
                .withExpressionAttributeValues(buildExpressionAttributeValues(hashKey));
    }

    private String buildConditionExpression(Class objClazz) {
        Field[] declaredFields = objClazz.getDeclaredFields();
        String hashKeyAttrName = null;
        for(Field field : declaredFields) {
            boolean hashKeyAnnotationPresent = field.isAnnotationPresent(DynamoDBHashKey.class);
            boolean isLong = field.getType().isAssignableFrom(Long.class);
            if(hashKeyAnnotationPresent && isLong) {
                hashKeyAttrName = field.getName();
            }
        }
        if(hashKeyAttrName == null) {
            throw new IllegalArgumentException("No DynamoDBHashKey annotation found in class: " + objClazz.getName());
        }
        return hashKeyAttrName + " = :v_id";
    }

    private Map<String, AttributeValue> buildExpressionAttributeValues(Long hashKey) {
        Map<String, AttributeValue> eav = new HashMap<>();
        eav.put(":v_id", new AttributeValue().withN(String.valueOf(hashKey)));
        return eav;
    }
}
