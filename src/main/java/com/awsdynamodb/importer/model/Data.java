package com.awsdynamodb.importer.model;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

/**
 * Created by ahadcse on 25/05/16.
 */
@DynamoDBTable(tableName="dynamodb-physical-table-name")
public class Data {

    @DynamoDBHashKey
    private Long data1;
    private String data2;
    private String data3;
    private Long data4;

    //This constructor is needed. Otherwise it might not work
    public Data() {
    }

    public Data(Long data1, String data2, String data3, Long data4) {
        this.data1 = data1;
        this.data2 = data2;
        this.data3 = data3;
        this.data4 = data4;
    }

    public Long getData1() {
        return data1;
    }

    public String getData2() {
        return data2;
    }

    public String getData3() {
        return data3;
    }

    public Long getData4() {
        return data4;
    }
}
