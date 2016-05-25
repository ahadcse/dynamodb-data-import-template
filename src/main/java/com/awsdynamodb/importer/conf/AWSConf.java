package com.awsdynamodb.importer.conf;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.amazonaws.SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY;
import static com.amazonaws.SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY;

@Configuration
public class AWSConf {

    private Logger LOGGER = LoggerFactory.getLogger(AWSConf.class);

    @Value("${amazon.aws.accesskey}")
    private String amazonAWSAccessKey;

    @Value("${amazon.aws.secretkey}")
    private String amazonAWSSecretKey;

    @Value("${amazon.aws.region.name}")
    private String regionName;

    @Value("${amazon.aws.role}")
    private String awsRoleARN;

    @Value("${amazon.aws.role.session.name}")
    private String roleSessionName;

    @Value("${importer.proxy.host}")
    private String proxyHost;

    @Value("${importer.proxy.port}")
    private Integer proxyPort;

    /**
     * Create bean of AWSCredentialsProvider
     *
     * The aws role is used to create a STSAssumeRoleSessionCredentialsProvider in order to get the short live credential
     *
     * Need to set the system properties ACCESS_KEY_SYSTEM_PROPERTY and SECRET_KEY_SYSTEM_PROPERTY in order to SystemPropertiesCredentialsProvider
     * for long live credentials.
     *
     * @return The AWSCredentialsProvider which can be used for creating AWSClient
     */
    @Bean
    public AWSCredentialsProvider awsCredentialsProvider() {
        System.setProperty(ACCESS_KEY_SYSTEM_PROPERTY,amazonAWSAccessKey);
        System.setProperty(SECRET_KEY_SYSTEM_PROPERTY,amazonAWSSecretKey);
        AWSCredentialsProvider longLivedCredentialsProvider = new SystemPropertiesCredentialsProvider();
        if(awsRoleARN != null && !awsRoleARN.isEmpty()) {
            return new STSAssumeRoleSessionCredentialsProvider(longLivedCredentialsProvider, awsRoleARN, roleSessionName, clientConfiguration());
        }
        return longLivedCredentialsProvider;
    }

    /**
     * Create bean of Region
     *
     * @return Region bean
     */
    @Bean
    public Region region() {
        if(regionName != null && !awsRoleARN.isEmpty()) {
            Region region = Region.getRegion(Regions.fromName(regionName));
            return region;
        } else {
            LOGGER.warn("AWS region name is not configured.");
            return null;
        }
    }

    /**
     * Create bean of ClientConfiguration
     * Set all the customized properties of AWSClient.
     * Currently, we need to set the http proxy in order to connect to AWS from Acc and Prod servers in Uppsala
     *
     * @return ClientConfiguration bean
     */
    @Bean
    public ClientConfiguration clientConfiguration() {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        if (proxyHost != null && !proxyHost.isEmpty() && proxyPort != null) {
            clientConfiguration.setProxyHost(proxyHost);
            clientConfiguration.setProxyPort(proxyPort);
        }
        return clientConfiguration;
    }
}
