package engineering.everest.starterkit.filestorage.filestores;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.InputStream;
import java.util.List;
import java.util.UUID;

import static engineering.everest.starterkit.filestorage.NativeStorageType.AWS_S3;
import static java.util.Arrays.asList;
import static java.util.Set.of;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    public void uploadStream_WillCreateUniquelyNamedS3Object() {
        var mockInputStream = mock(InputStream.class);
        var fileIdentifier = fileStore.uploadStream(mockInputStream, "fileName");
        // Would have been a clean test if the UUID wrapper hadn't been removed
        var randomUUIDPart = UUID.fromString(fileIdentifier.substring(fileIdentifier.indexOf('-') + 1));

        assertEquals(String.format("s3://bucket/fileName-%s", randomUUIDPart), fileIdentifier);
        verify(amazonS3).putObject("bucket", String.format("fileName-%s", randomUUIDPart), mockInputStream, null);
    }

    @Test
    public void uploadStreamWillFileSize_WillCreateUniquelyNamedS3Object() {
        var mockInputStream = mock(InputStream.class);
        var fileIdentifier = fileStore.uploadStream(mockInputStream, "fileName", 4543L);
        // Would have been a clean test if the UUID wrapper hadn't been removed
        var randomUUIDPart = UUID.fromString(fileIdentifier.substring(fileIdentifier.indexOf('-') + 1));

        assertEquals(String.format("s3://bucket/fileName-%s", randomUUIDPart), fileIdentifier);
        verify(amazonS3).putObject(eq("bucket"), eq(String.format("fileName-%s", randomUUIDPart)),
            eq(mockInputStream), any(ObjectMetadata.class));
    }

    @Test
    public void delete_WillDeleteFromTheS3Bucket() {
        fileStore.delete("s3://bucket/fileName");

        verify(amazonS3).deleteObject("bucket", "fileName");
    }

    @Test
    public void downloadAsStream_WillFailWhenTheObjectDoesNotExistInTheS3Bucket() {
        var exception = assertThrows(RuntimeException.class, () -> fileStore.downloadAsStream("s3://bucket/fileName"));
        assertEquals("Unable to retrieve file: s3://bucket/fileName", exception.getMessage());
    }

    @Test
    public void downloadAsStream_WillFetchTheObjectFromS3Bucket() {
        var fileIdentifier = "s3://bucket/fileName";
        var mockS3Object = mock(S3Object.class);
        ObjectMetadata objectMetadata = mock(ObjectMetadata.class);
        when(mockS3Object.getObjectMetadata()).thenReturn(objectMetadata);
        when(objectMetadata.getInstanceLength()).thenReturn(10L);

        when(amazonS3.doesObjectExist("bucket", "fileName")).thenReturn(true);
        when(amazonS3.getObject("bucket", "fileName")).thenReturn(mockS3Object);

        var inputStreamOfKnownLength = fileStore.downloadAsStream(fileIdentifier);
        assertEquals(10L, inputStreamOfKnownLength.getLength());
        verify(amazonS3).getObject("bucket", "fileName");
    }

    @Test
    public void nativeStorageTypeIsAwsS3() {
        assertEquals(AWS_S3, fileStore.nativeStorageType());
    }

    @Test
    public void deleteFiles_WillDeleteFromTheS3Bucket() {
        var fileIdentifiers = of("s3://bucket/fileName");

        fileStore.deleteFiles(fileIdentifiers);

        ArgumentCaptor<DeleteObjectsRequest> captor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        new DeleteObjectsRequest("bucket")
            .withKeys(asList(new DeleteObjectsRequest.KeyVersion("fileName", "L4kqtJlcpXroDTDmpUMLUo")))
            .withQuiet(false);
        verify(amazonS3).deleteObjects(captor.capture());

        DeleteObjectsRequest value = captor.getValue();
        List<DeleteObjectsRequest.KeyVersion> keys = value.getKeys();
        assertEquals("bucket", value.getBucketName());
        assertFalse(value.getQuiet());
        assertEquals("fileName", keys.get(0).getKey());
        assertNull(keys.get(0).getVersion());
    }
}
