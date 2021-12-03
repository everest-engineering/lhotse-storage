package engineering.everest.starterkit.filestorage.filestores;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import static engineering.everest.starterkit.filestorage.NativeStorageType.IN_MEMORY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class InMemoryFileStoreTest {

    private static final String FILENAME = "my-filename";
    private static byte[] FILE_CONTENTS = "This is my file. There are many like it but this one is my own.".getBytes();

    private InMemoryFileStore inMemoryFileStore;

    @BeforeEach
    void setUp() {
        inMemoryFileStore = new InMemoryFileStore();
    }

    @Test
    void nativeStorageType_WillBeInMemory() {
        assertEquals(IN_MEMORY, inMemoryFileStore.nativeStorageType());
    }

    @Test
    void uploadStreamWithFileSize_WillPersistFile() throws IOException {
        var persistedFileId = inMemoryFileStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME, FILE_CONTENTS.length);
        var downloadedFileContents = inMemoryFileStore.downloadAsStream(persistedFileId).getInputStream().readAllBytes();

        assertArrayEquals(FILE_CONTENTS, downloadedFileContents);
    }

    @Test
    void uploadStreamWithFileSize_WillFail_WhenUploadInputStreamFails() throws IOException {
        var explodingInputStream = mock(InputStream.class);
        when(explodingInputStream.readAllBytes()).thenThrow(new IOException("you can't handle the truth"));

        var exception = assertThrows(RuntimeException.class,
            () -> inMemoryFileStore.uploadStream(explodingInputStream, FILENAME, FILE_CONTENTS.length));

        assertEquals("Unable to upload file my-filename", exception.getMessage());
    }

    @Test
    void uploadStreamWithFileSize_WillRecordContentLength() throws IOException {
        var persistedFileId = inMemoryFileStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME, FILE_CONTENTS.length);

        assertEquals(FILE_CONTENTS.length, inMemoryFileStore.downloadAsStream(persistedFileId).getLength());
    }

    @Test
    void uploadStreamWithoutFileSize_WillPersistFile() throws IOException {
        var persistedFileId = inMemoryFileStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME);
        var downloadedFileContents = inMemoryFileStore.downloadAsStream(persistedFileId).getInputStream().readAllBytes();

        assertArrayEquals(FILE_CONTENTS, downloadedFileContents);
    }

    @Test
    void uploadStreamWithoutFileSize_WillRecordContentLength() throws IOException {
        var persistedFileId = inMemoryFileStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME);

        assertEquals(FILE_CONTENTS.length, inMemoryFileStore.downloadAsStream(persistedFileId).getLength());
    }

    @Test
    void uploadStreamWithoutFileSize_WillFail_WhenExpectedFileSizeDiffersFromContentLength() {
        var exception = assertThrows(RuntimeException.class,
            () -> inMemoryFileStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME, FILE_CONTENTS.length + 1));
        assertEquals("Expected file size 64 for uploaded file 'my-filename' but content length is 63", exception.getMessage());
    }

    @Test
    void uploadStreamWithoutFileSize_WillFail_WhenUploadInputStreamFails() throws IOException {
        var explodingInputStream = mock(InputStream.class);
        when(explodingInputStream.readAllBytes()).thenThrow(new IOException("you can't handle the truth"));

        var exception = assertThrows(RuntimeException.class, () -> inMemoryFileStore.uploadStream(explodingInputStream, FILENAME));

        assertEquals("Unable to upload file my-filename", exception.getMessage());
    }

    @Test
    void delete_WillOnlyDeleteSpecifiedFile() throws IOException {
        var persistedFileId1 = inMemoryFileStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME);
        var persistedFileId2 = inMemoryFileStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME);

        inMemoryFileStore.delete(persistedFileId1);
        assertThrows(RuntimeException.class, () -> inMemoryFileStore.downloadAsStream(persistedFileId1));
        assertNotNull(inMemoryFileStore.downloadAsStream(persistedFileId2).getInputStream());
    }

    @Test
    void delete_WillFail_WhenFileIsNotInStore() {
        var persistedFileId = inMemoryFileStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME);
        inMemoryFileStore.delete(persistedFileId);

        var exception = assertThrows(RuntimeException.class, () -> inMemoryFileStore.delete(persistedFileId));

        assertEquals(String.format("File '%s' not in filestore", persistedFileId), exception.getMessage());
    }

    @Test
    void deleteFiles_WillDeleteAllSpecifiedFiles() {
        var persistedFileId1 = inMemoryFileStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME);
        var persistedFileId2 = inMemoryFileStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME);

        inMemoryFileStore.deleteFiles(Set.of(persistedFileId1, persistedFileId2));
    }

    @Test
    void deleteFiles_WillFail_WhenAFileIsNotInStore() {
        var exception = assertThrows(RuntimeException.class, () -> inMemoryFileStore.deleteFiles(Set.of("garbage-id")));
        assertEquals("File 'garbage-id' not in filestore", exception.getMessage());
    }

    @Test
    void deleteFiles_WillNotDeleteAnyFiles_WhenAnyFileIsNotInStore() throws IOException {
        var persistedFileId = inMemoryFileStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME);

        assertThrows(RuntimeException.class, () -> inMemoryFileStore.deleteFiles(Set.of(persistedFileId, "garbage-id")));
        assertNotNull(inMemoryFileStore.downloadAsStream(persistedFileId));
    }
}
