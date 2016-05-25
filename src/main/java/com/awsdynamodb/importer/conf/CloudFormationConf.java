package com.awsdynamodb.importer.conf;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.AmazonCloudFormationClient;
import com.awsdynamodb.importer.service.CloudFormationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.UnaryOperator;

@Configuration
public class CloudFormationConf {

    private Logger LOGGER = LoggerFactory.getLogger(CloudFormationConf.class);

    @Value("${amazon.cloud.formation.lookup.enable}")
    private boolean cloudFormationEnabled;

    @Autowired
    private AWSCredentialsProvider awsCredentialsProvider;

    @Autowired
    private ClientConfiguration clientConfiguration;

    @Autowired
    private Region region;

    /**
     * Create bean of AmazonCloudFormation
     *
     * Only create AmazonCloudFormation object if the property amazon.cloud.formation.lookup.enable is set to true
     * Otherwise, return null
     *
     * If we need to lookup physical id by logical name then we need to set the property to true. For instance, the name
     * of DynamoDb table. Then the physical id will be returned as  If the value of property is false, then the table name that annotated on the Pojo class will be used
     * directly when doing operations.
     *
     * @return
     */
    @Bean
    public AmazonCloudFormation amazonCloudFormation() {
        if (cloudFormationEnabled) {
            AmazonCloudFormation stackBuilder = new AmazonCloudFormationClient(awsCredentialsProvider, clientConfiguration);
            stackBuilder.setRegion(region);
            return stackBuilder;
        } else {
            LOGGER.warn("AWS CloudFormation is not enabled.");
            return null;
        }
    }

    @Bean
    public UnaryOperator<String> cloudFormationService() {
        return new CloudFormationService(amazonCloudFormation());
    }
}
