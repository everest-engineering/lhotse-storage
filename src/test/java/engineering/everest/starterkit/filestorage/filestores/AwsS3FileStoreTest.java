package engineering.everest.starterkit.filestorage.filestores;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import static engineering.everest.starterkit.filestorage.NativeStorageType.AWS_S3;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AwsS3FileStoreTest {

    private AwsS3FileStore fileStore;

    @Mock
    private AmazonS3 amazonS3;

    @BeforeEach
    public void setUp() {
        this.fileStore = new AwsS3FileStore(amazonS3, "bucket");
    }

    @Test
    public void create_CreatesUniquelyNamedObjectInTheS3Bucket() {
        var mockInputStream = mock(InputStream.class);
        var fileIdentifier = fileStore.create(mockInputStream, "fileName");
        // Would have been a clean test if the UUID wrapper hadn't been removed
        var randomUUIDPart = UUID.fromString(fileIdentifier.substring(fileIdentifier.indexOf('-') + 1));

        assertEquals(String.format("s3://bucket/fileName-%s", randomUUIDPart), fileIdentifier);
        verify(amazonS3).putObject("bucket", String.format("fileName-%s", randomUUIDPart), mockInputStream, null);
    }

    @Test
    public void delete_DeletesTheObjectFromS3Bucket() {
        fileStore.delete("s3://bucket/fileName");

        verify(amazonS3).deleteObject("bucket", "fileName");
    }

    @Test
    public void read_FailsIfTheObjectDoesNotExistInTheS3Bucket() {
        var exception = assertThrows(RuntimeException.class, () -> fileStore.read("s3://bucket/fileName"));
        assertEquals("Unable to retrieve file: s3://bucket/fileName", exception.getMessage());
    }

    @Test
    public void read_FetchesTheObjectFromS3Bucket() {
        var fileIdentifier = "s3://bucket/fileName";
        var mockS3Object = mock(S3Object.class);
        ObjectMetadata objectMetadata = mock(ObjectMetadata.class);
        when(mockS3Object.getObjectMetadata()).thenReturn(objectMetadata);
        when(objectMetadata.getInstanceLength()).thenReturn(10L);

        when(amazonS3.doesObjectExist("bucket", "fileName")).thenReturn(true);
        when(amazonS3.getObject("bucket", "fileName")).thenReturn(mockS3Object);

        var inputStreamOfKnownLength = fileStore.read(fileIdentifier);
        assertEquals(10L, inputStreamOfKnownLength.getLength());
        verify(amazonS3).getObject("bucket", "fileName");
    }

    @Test
    public void nativeStorageTypeIsAwsS3() {
        assertEquals(AWS_S3, fileStore.nativeStorageType());
    }
}