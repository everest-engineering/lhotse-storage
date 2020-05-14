package engineering.everest.starterkit.filestorage.filestores;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import engineering.everest.starterkit.filestorage.FileStore;
import engineering.everest.starterkit.filestorage.InputStreamOfKnownLength;
import engineering.everest.starterkit.filestorage.NativeStorageType;

import java.io.InputStream;
import java.util.List;
import java.util.Set;

import static engineering.everest.starterkit.filestorage.NativeStorageType.AWS_S3;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;

public class AwsS3FileStore implements FileStore {

    private AmazonS3 amazonS3;
    private String bucketName;

    public AwsS3FileStore(AmazonS3 amazonS3, String bucketName) {
        this.amazonS3 = amazonS3;
        this.bucketName = bucketName;
    }

    @Override
    public String uploadStream(InputStream inputStream, String fileName) {
        return streamToS3(inputStream, fileName, null);
    }

    @Override
    public String uploadStream(InputStream inputStream, String fileName, long fileSize) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(fileSize);
        return streamToS3(inputStream, fileName, metadata);
    }

    @Override
    public void delete(String fileIdentifier) {
        AmazonS3URI s3URI = new AmazonS3URI(fileIdentifier);
        amazonS3.deleteObject(s3URI.getBucket(), s3URI.getKey());
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public InputStreamOfKnownLength downloadAsStream(String fileIdentifier) {
        AmazonS3URI s3URI = new AmazonS3URI(fileIdentifier);
        if (amazonS3.doesObjectExist(s3URI.getBucket(), s3URI.getKey())) {
            S3Object s3Object = amazonS3.getObject(s3URI.getBucket(), s3URI.getKey());
            return new InputStreamOfKnownLength(s3Object.getObjectContent(), s3Object.getObjectMetadata().getInstanceLength());
        } else {
            throw new RuntimeException(String.format("Unable to retrieve file: %s", fileIdentifier));
        }
    }

    @Override
    public NativeStorageType nativeStorageType() {
        return AWS_S3;
    }

    @Override
    public void deleteFiles(Set<String> fileIdentifiers) {
        List<KeyVersion> keyVersions = fileIdentifiers.stream().map(fileIdentifier -> {
            AmazonS3URI s3URI = new AmazonS3URI(fileIdentifier);
            return new KeyVersion(s3URI.getKey(), s3URI.getVersionId());
        }).collect(toList());
        DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(bucketName)
                .withKeys(keyVersions)
                .withQuiet(false);
        amazonS3.deleteObjects(multiObjectDeleteRequest);
    }

    private String streamToS3(InputStream inputStream, String fileName, ObjectMetadata o) {
        String uniqueS3Filename = ensureFilenameIsUniqueForS3(fileName);
        amazonS3.putObject(bucketName, uniqueS3Filename, inputStream, o);
        return String.format("s3://%s/%s", bucketName, uniqueS3Filename);
    }

    private String ensureFilenameIsUniqueForS3(String filename) {
        return String.format("%s-%s", filename, randomUUID());
    }
}
