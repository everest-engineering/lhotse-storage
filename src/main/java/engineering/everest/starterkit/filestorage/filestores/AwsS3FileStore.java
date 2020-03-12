package engineering.everest.starterkit.filestorage.filestores;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;
import engineering.everest.starterkit.filestorage.FileStore;
import engineering.everest.starterkit.filestorage.InputStreamOfKnownLength;
import engineering.everest.starterkit.filestorage.NativeStorageType;

import java.io.InputStream;

import static engineering.everest.starterkit.filestorage.NativeStorageType.AWS_S3;
import static java.util.UUID.randomUUID;

public class AwsS3FileStore implements FileStore {

    private AmazonS3 amazonS3;
    private String bucketName;

    public AwsS3FileStore(AmazonS3 amazonS3, String bucketName) {
        this.amazonS3 = amazonS3;
        this.bucketName = bucketName;
    }

    @Override
    public String create(InputStream inputStream, String fileName) {
        String uniqueS3Filename = ensureFilenameIsUniqueForS3(fileName);
        amazonS3.putObject(bucketName, uniqueS3Filename, inputStream, null);
        return String.format("s3://%s/%s", bucketName, uniqueS3Filename);
    }

    @Override
    public void delete(String fileIdentifier) {
        AmazonS3URI s3URI = new AmazonS3URI(fileIdentifier);
        amazonS3.deleteObject(s3URI.getBucket(), s3URI.getKey());
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public InputStreamOfKnownLength read(String fileIdentifier) {
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

    private String ensureFilenameIsUniqueForS3(String filename) {
        return String.format("%s-%s", filename, randomUUID());
    }
}
