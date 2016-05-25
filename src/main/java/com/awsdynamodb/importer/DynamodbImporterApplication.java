package com.awsdynamodb.importer;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.awsdynamodb.importer.client.DynamoDBClient;
import com.awsdynamodb.importer.model.Data;
import com.google.common.collect.Iterators;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
public class DynamodbImporterApplication implements CommandLineRunner {

    private static final String IMPORT_FILE_DIRECTORY = "data.csv";
    private static final int BATCH_SIZE = 25;

    @Autowired
    DynamoDBClient dynamoDBClient;

    public static void main(String[] args) {
        SpringApplication.run(DynamodbImporterApplication.class, args);
    }

    @Override
    public void run(String... strings) throws Exception {
        try (Stream<String> stream = Files.lines(Paths.get(IMPORT_FILE_DIRECTORY))) {

            Iterators.partition(stream.iterator(), BATCH_SIZE).forEachRemaining(
                    list -> {
                        List<Data> datas = list.stream()
                                .map(JSONObject::new)
                                .map((obj) -> {
                                    JSONObject data1Obj = (JSONObject) obj.get("data1");
                                    JSONObject data2Obj = (JSONObject) obj.get("data2");
                                    JSONObject data3Obj = (JSONObject) obj.get("data3");
                                    JSONObject data4Obj = (JSONObject) obj.get("data4");
                                    return new Data(data1Obj.getLong("n"),
                                            data2Obj.getString("s"),
                                            data3Obj.getString("s"),
                                            data4Obj.getString("n").equals("") ? null : data4Obj.getLong("n"));
                                }).collect(Collectors.toList());
                        System.out.println("Processing data: " + datas.stream().map(Data::getData1).collect(Collectors.toList()));
                        List<DynamoDBMapper.FailedBatch> failedBatches = dynamoDBClient.batchInsertData(datas);
                        if(!failedBatches.isEmpty()) {
                            System.out.println("*** " + failedBatches.size() + "***");
                        }
                    }
            );

        }
    }
}
