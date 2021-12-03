package engineering.everest.starterkit.filestorage.backing;

import engineering.everest.starterkit.filestorage.BackingStorageType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.gridfs.GridFsTemplate;

import java.io.InputStream;

import static java.util.Set.of;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

@ExtendWith(MockitoExtension.class)
class MongoGridFsBackingStoreTest {

    private MongoGridFsBackingStore fileStore;

    @Mock
    private GridFsTemplate gridFsTemplate;

    @BeforeEach
    public void setUp() {
        this.fileStore = new MongoGridFsBackingStore(gridFsTemplate);
    }

    @Test
    public void uploadStream_WillStoreFileInGridFs() {
        var objectId = new ObjectId();
        var mockInputStream = mock(InputStream.class);
        when(gridFsTemplate.store(mockInputStream, "file")).thenReturn(objectId);

        assertEquals(fileStore.uploadStream(mockInputStream, "file"), objectId.toHexString());
    }

    @Test
    public void uploadStreamWithFileSize_WillStoreFileInGridFs() {
        var objectId = new ObjectId();
        var mockInputStream = mock(InputStream.class);
        when(gridFsTemplate.store(mockInputStream, "file")).thenReturn(objectId);

        assertEquals(fileStore.uploadStream(mockInputStream, "file", 4343L), objectId.toHexString());
    }

    @Test
    public void delete_WillRemoveFileFromGridFs() {
        String fileIdentifier = "5e253b753496211048764352";
        fileStore.delete(fileIdentifier);

        verify(gridFsTemplate).delete(query(where("_id").is(new ObjectId(fileIdentifier))));
    }

    @Test
    public void downloadAsStream_WillFailWhenFilesDoesNotExist() {
        var exception = assertThrows(RuntimeException.class, () -> fileStore.downloadAsStream("5e253b753496211048764352"));

        assertEquals(exception.getMessage(), "Unable to retrieve file 5e253b753496211048764352");
    }

    @Test
    public void backingStorageTypeIsMongoGridFs() {
        assertEquals(this.fileStore.backingStorageType(), BackingStorageType.MONGO_GRID_FS);
    }

    @Test
    public void deleteFiles_WillRemoveFilesFromGridFs() {
        var fileIdentifiers = of("5e253b753496211048764352", "5e253b753496211048764351");
        fileStore.deleteFiles(fileIdentifiers);

        verify(gridFsTemplate).delete(query(where("_id").in(fileIdentifiers)));
    }
}
