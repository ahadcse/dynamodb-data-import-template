package com.awsdynamodb.importer.conf;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.awsdynamodb.importer.client.DynamoDBClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.UnaryOperator;

@Configuration
public class DynamoDBConf {

    @Value("${amazon.dynamodb.endpoint}")
    private String amazonDynamoDBEndpoint;

    @Autowired
    private AWSCredentialsProvider awsCredentialsProvider;

    @Autowired
    private ClientConfiguration clientConfiguration;

    @Autowired
    private Region region;

    @Autowired
    private UnaryOperator<String> cloudFormationService;

    @Bean
    public DynamoDBMapper dynamoDBMapper() {
        return new DynamoDBMapper(amazonDynamoDBClient());
    }

    @Bean
    public DynamoDB dynamoDB() {
        DynamoDB dynamodb = new DynamoDB(amazonDynamoDBClient());
        return dynamodb;
    }

    @Bean
    public AmazonDynamoDB amazonDynamoDBClient() {
        AmazonDynamoDBClient client = new AmazonDynamoDBClient(awsCredentialsProvider, clientConfiguration);
        if (amazonDynamoDBEndpoint != null && !amazonDynamoDBEndpoint.isEmpty()) {
            client.setEndpoint(amazonDynamoDBEndpoint);
        }
        if (region != null) {
            client.setRegion(region);
        }
        return client;
    }

    @Bean
    DynamoDBClient dynamoDBClient() {
        return new DynamoDBClient(dynamoDBMapper(), cloudFormationService);
    }
}
