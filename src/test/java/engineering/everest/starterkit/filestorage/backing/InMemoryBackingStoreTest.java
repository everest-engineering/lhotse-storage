package engineering.everest.starterkit.filestorage.backing;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import static engineering.everest.starterkit.filestorage.BackingStorageType.IN_MEMORY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class InMemoryBackingStoreTest {

    private static final String FILENAME = "my-filename";
    private static byte[] FILE_CONTENTS = "This is my file. There are many like it but this one is my own.".getBytes();

    private InMemoryBackingStore inMemoryBackingStore;

    @BeforeEach
    void setUp() {
        inMemoryBackingStore = new InMemoryBackingStore();
    }

    @Test
    void backingStorageType_WillBeInMemory() {
        assertEquals(IN_MEMORY, inMemoryBackingStore.backingStorageType());
    }

    @Test
    void uploadStreamWithFileSize_WillPersistFile() throws IOException {
        var persistedFileId = inMemoryBackingStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME, FILE_CONTENTS.length);
        var downloadedFileContents = inMemoryBackingStore.downloadAsStream(persistedFileId).getInputStream().readAllBytes();

        assertArrayEquals(FILE_CONTENTS, downloadedFileContents);
    }

    @Test
    void uploadStreamWithFileSize_WillFail_WhenUploadInputStreamFails() throws IOException {
        var explodingInputStream = mock(InputStream.class);
        when(explodingInputStream.readAllBytes()).thenThrow(new IOException("you can't handle the truth"));

        var exception = assertThrows(RuntimeException.class,
            () -> inMemoryBackingStore.uploadStream(explodingInputStream, FILENAME, FILE_CONTENTS.length));

        assertEquals("Unable to upload file my-filename", exception.getMessage());
    }

    @Test
    void uploadStreamWithFileSize_WillRecordContentLength() throws IOException {
        var persistedFileId = inMemoryBackingStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME, FILE_CONTENTS.length);

        assertEquals(FILE_CONTENTS.length, inMemoryBackingStore.downloadAsStream(persistedFileId).getLength());
    }

    @Test
    void uploadStreamWithoutFileSize_WillPersistFile() throws IOException {
        var persistedFileId = inMemoryBackingStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME);
        var downloadedFileContents = inMemoryBackingStore.downloadAsStream(persistedFileId).getInputStream().readAllBytes();

        assertArrayEquals(FILE_CONTENTS, downloadedFileContents);
    }

    @Test
    void uploadStreamWithoutFileSize_WillRecordContentLength() throws IOException {
        var persistedFileId = inMemoryBackingStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME);

        assertEquals(FILE_CONTENTS.length, inMemoryBackingStore.downloadAsStream(persistedFileId).getLength());
    }

    @Test
    void uploadStreamWithoutFileSize_WillFail_WhenExpectedFileSizeDiffersFromContentLength() {
        var exception = assertThrows(RuntimeException.class,
            () -> inMemoryBackingStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME, FILE_CONTENTS.length + 1));
        assertEquals("Expected file size 64 for uploaded file 'my-filename' but content length is 63", exception.getMessage());
    }

    @Test
    void uploadStreamWithoutFileSize_WillFail_WhenUploadInputStreamFails() throws IOException {
        var explodingInputStream = mock(InputStream.class);
        when(explodingInputStream.readAllBytes()).thenThrow(new IOException("you can't handle the truth"));

        var exception = assertThrows(RuntimeException.class, () -> inMemoryBackingStore.uploadStream(explodingInputStream, FILENAME));

        assertEquals("Unable to upload file my-filename", exception.getMessage());
    }

    @Test
    void delete_WillOnlyDeleteSpecifiedFile() throws IOException {
        var persistedFileId1 = inMemoryBackingStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME);
        var persistedFileId2 = inMemoryBackingStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME);

        inMemoryBackingStore.delete(persistedFileId1);
        assertThrows(RuntimeException.class, () -> inMemoryBackingStore.downloadAsStream(persistedFileId1));
        assertNotNull(inMemoryBackingStore.downloadAsStream(persistedFileId2).getInputStream());
    }

    @Test
    void deleteIsIdempotent() {
        var persistedFileId = inMemoryBackingStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME);
        inMemoryBackingStore.delete(persistedFileId);
        inMemoryBackingStore.delete(persistedFileId);
        assertThrows(RuntimeException.class, () -> inMemoryBackingStore.downloadAsStream(persistedFileId));
    }

    @Test
    void deleteFiles_WillDeleteAllSpecifiedFiles() {
        var persistedFileId1 = inMemoryBackingStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME);
        var persistedFileId2 = inMemoryBackingStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME);

        inMemoryBackingStore.deleteFiles(Set.of(persistedFileId1, persistedFileId2));
    }

    @Test
    void deleteFiles_WillFail_WhenAFileIsNotInStore() {
        var exception = assertThrows(RuntimeException.class, () -> inMemoryBackingStore.deleteFiles(Set.of("garbage-id")));
        assertEquals("File 'garbage-id' not in filestore", exception.getMessage());
    }

    @Test
    void deleteFiles_WillNotDeleteAnyFiles_WhenAnyFileIsNotInStore() throws IOException {
        var persistedFileId = inMemoryBackingStore.uploadStream(new ByteArrayInputStream(FILE_CONTENTS), FILENAME);

        assertThrows(RuntimeException.class, () -> inMemoryBackingStore.deleteFiles(Set.of(persistedFileId, "garbage-id")));
        assertNotNull(inMemoryBackingStore.downloadAsStream(persistedFileId));
    }
}
