package com.awsdynamodb.importer.service;

import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.model.DescribeStackResourceRequest;
import com.amazonaws.services.cloudformation.model.StackResourceDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Service class to interact with CloudFormation service.
 */
public class CloudFormationService implements UnaryOperator<String> {

    private Logger LOGGER = LoggerFactory.getLogger(CloudFormationService.class);

    private final AmazonCloudFormation amazonCloudFormation;

    @Value("${amazon.cloud.formation.stack.name.dynamodb}")
    private String stackName;

    /**
     * Local map to keep the mapping between logical name and physical id to avoid retrieve physical id from CloudFormation everytime.
     */
    private Map<String, String> localMap;

    public CloudFormationService(AmazonCloudFormation amazonCloudFormation) {
        this.amazonCloudFormation = amazonCloudFormation;
    }


    @PostConstruct
    public void init() {
        localMap = new HashMap<>();
    }

    @Override
    public String apply(String logicalResourceName) {
        if (amazonCloudFormation != null) {
            String physicalId = localMap.get(logicalResourceName);
            if (physicalId == null || physicalId.isEmpty()) { //Use CloudFormation SDK to retrieve the p
                DescribeStackResourceRequest logicalNameResourceRequest = new DescribeStackResourceRequest();
                logicalNameResourceRequest.setStackName(stackName);
                logicalNameResourceRequest.setLogicalResourceId(logicalResourceName);
                LOGGER.debug("Looking up resource name {} from stack {} ", logicalNameResourceRequest.getLogicalResourceId(), logicalNameResourceRequest.getStackName());
                StackResourceDetail stackResourceDetail = amazonCloudFormation.describeStackResource(logicalNameResourceRequest).getStackResourceDetail();
                if (stackResourceDetail != null) {
                    localMap.put(logicalResourceName, stackResourceDetail.getPhysicalResourceId());
                    return stackResourceDetail.getPhysicalResourceId();
                } else {
                    throw new RuntimeException("Cannot find stack resource by logical name  " + logicalResourceName);
                }
            } else {
                LOGGER.debug("Get physical id {} from local map for logical name {} , use it. ", physicalId, logicalResourceName);
                return physicalId;
            }
        }
        return logicalResourceName;
    }

    protected void setLocalMap(Map<String, String> localMap) {
        this.localMap = localMap;
    }

}
