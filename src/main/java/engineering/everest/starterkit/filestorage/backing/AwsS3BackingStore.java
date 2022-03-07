package engineering.everest.starterkit.filestorage.backing;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import engineering.everest.starterkit.filestorage.BackingStore;
import engineering.everest.starterkit.filestorage.InputStreamOfKnownLength;
import engineering.everest.starterkit.filestorage.BackingStorageType;

import java.io.InputStream;
import java.util.Set;

import static engineering.everest.starterkit.filestorage.BackingStorageType.AWS_S3;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;

public class AwsS3BackingStore implements BackingStore {

    private final AmazonS3 amazonS3;
    private final String bucketName;

    public AwsS3BackingStore(AmazonS3 amazonS3, String bucketName) {
        this.amazonS3 = amazonS3;
        this.bucketName = bucketName;
    }

    @Override
    public String uploadStream(InputStream inputStream, String fileName) {
        return streamToS3(inputStream, fileName, null);
    }

    @Override
    public String uploadStream(InputStream inputStream, String fileName, long fileSize) {
        var metadata = new ObjectMetadata();
        metadata.setContentLength(fileSize);
        return streamToS3(inputStream, fileName, metadata);
    }

    @Override
    public void delete(String fileIdentifier) {
        var s3URI = new AmazonS3URI(fileIdentifier);
        amazonS3.deleteObject(s3URI.getBucket(), s3URI.getKey());
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public InputStreamOfKnownLength downloadAsStream(String fileIdentifier) {
        var s3URI = new AmazonS3URI(fileIdentifier);
        if (amazonS3.doesObjectExist(s3URI.getBucket(), s3URI.getKey())) {
            var s3Object = amazonS3.getObject(s3URI.getBucket(), s3URI.getKey());
            return new InputStreamOfKnownLength(s3Object.getObjectContent(), s3Object.getObjectMetadata().getInstanceLength());
        } else {
            throw new BackingFileStoreException(String.format("Unable to retrieve file: %s", fileIdentifier));
        }
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public InputStreamOfKnownLength downloadAsStream(String fileIdentifier, long startingOffset, long endingOffset) {
        var s3URI = new AmazonS3URI(fileIdentifier);
        if (amazonS3.doesObjectExist(s3URI.getBucket(), s3URI.getKey())) {
            var objectRequest = new GetObjectRequest(s3URI.getBucket(), s3URI.getKey());
            objectRequest.setRange(startingOffset, endingOffset);
            var s3Object = amazonS3.getObject(objectRequest);
            return new InputStreamOfKnownLength(s3Object.getObjectContent(), s3Object.getObjectMetadata().getInstanceLength());
        } else {
            throw new BackingFileStoreException(String.format("Unable to retrieve file: %s", fileIdentifier));
        }
    }

    @Override
    public BackingStorageType backingStorageType() {
        return AWS_S3;
    }

    @Override
    public void deleteFiles(Set<String> fileIdentifiers) {
        var keyVersions = fileIdentifiers.stream().map(fileIdentifier -> {
            var s3URI = new AmazonS3URI(fileIdentifier);
            return new KeyVersion(s3URI.getKey(), s3URI.getVersionId());
        }).collect(toList());

        var multiObjectDeleteRequest = new DeleteObjectsRequest(bucketName)
            .withKeys(keyVersions)
            .withQuiet(false);
        amazonS3.deleteObjects(multiObjectDeleteRequest);
    }

    private String streamToS3(InputStream inputStream, String fileName, ObjectMetadata o) {
        var uniqueS3Filename = ensureFilenameIsUniqueForS3(fileName);
        amazonS3.putObject(bucketName, uniqueS3Filename, inputStream, o);
        return String.format("s3://%s/%s", bucketName, uniqueS3Filename);
    }

    private String ensureFilenameIsUniqueForS3(String filename) {
        return String.format("%s-%s", filename, randomUUID());
    }
}
