package engineering.everest.starterkit.filestorage.config;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.util.StringUtils;
import engineering.everest.starterkit.filestorage.FileStore;
import engineering.everest.starterkit.filestorage.filestores.AwsS3FileStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "application.filestore.backend", havingValue = "awsS3")
public class AwsS3FileStoreConfig {

    @Bean
    AmazonS3 s3Client(@Value("${application.filestore.awsS3.endpoint:}") String customEndpoint,
                      AWSCredentialsProvider awsCredentialsProvider) {
        if (StringUtils.isNullOrEmpty(customEndpoint)) {
            return AmazonS3ClientBuilder.defaultClient();
        }

        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(customEndpoint, regionFor(customEndpoint)))
                .withCredentials(awsCredentialsProvider)
                .build();
    }

    @Bean
    @Qualifier("permanentFileStore")
    FileStore awsS3PermanentFileStore(AmazonS3 s3Client, @Value("${application.filestore.awsS3.buckets.permanent}") String bucketName) {
        return new AwsS3FileStore(s3Client, bucketName);
    }

    @Bean
    @Qualifier("ephemeralFileStore")
    FileStore awsS3EphemeralFileStore(AmazonS3 s3Client, @Value("${application.filestore.awsS3.buckets.ephemeral}") String bucketName) {
        return new AwsS3FileStore(s3Client, bucketName);
    }

    private String regionFor(String customEndpoint) {
        return customEndpoint.substring(0, customEndpoint.indexOf('.'));
    }
}
