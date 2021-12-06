package engineering.everest.starterkit.filestorage;

import engineering.everest.starterkit.filestorage.persistence.FileMappingRepository;
import engineering.everest.starterkit.filestorage.persistence.PersistableFileMapping;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Example;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static engineering.everest.starterkit.filestorage.BackingStorageType.MONGO_GRID_FS;
import static engineering.everest.starterkit.filestorage.FileStoreType.PERMANENT;
import static java.nio.file.Files.createTempFile;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PermanentDeduplicatingFileStoreTest {

    private static final String ORIGINAL_FILENAME = "original-filename";
    private static final String EXISTING_BACKING_STORE_FILE_ID = "existing-backing-store-file-id";
    private static final String SHA_256 = "108e0047119fdf8db72dc146283d0cd717d620a9b4fb9ead902e22f4c04fbe7b";
    private static final String SHA_512 =
        "cb61c18674f50eedd4f7d77f938b11d468713516b14862c4ae4ea68ec5aa30c1475d7d38f17e14585da10ea848a054733f2185b1ea57f10a1c416bb1617baa60";
    private static final String TEMPORARY_FILE_CONTENTS = "A temporary file for unit testing";
    private static final Long FILE_SIZE = (long) TEMPORARY_FILE_CONTENTS.length();

    private PermanentDeduplicatingFileStore permanentDeduplicatingFileStore;
    private String fileIdentifier;

    @Mock
    protected BackingStore backingStore;
    @Mock
    protected FileMappingRepository fileMappingRepository;

    @BeforeEach
    void setUp() {
        permanentDeduplicatingFileStore = new PermanentDeduplicatingFileStore(fileMappingRepository, backingStore);
        fileIdentifier = "FILE_ID";
    }

    @Test
    void uploadAsStream_WillPersistAndReturnBackingStorageEncodingFileId() throws IOException {
        when(backingStore.backingStorageType()).thenReturn(MONGO_GRID_FS);
        when(backingStore.uploadStream(any(InputStream.class), eq(ORIGINAL_FILENAME))).thenAnswer(invocation -> {
            InputStream inputFile = invocation.getArgument(0);
            inputFile.readAllBytes();
            inputFile.close();
            return fileIdentifier;
        });

        PersistedFile persistedFile = permanentDeduplicatingFileStore.uploadAsStream(ORIGINAL_FILENAME, createTempFileWithContents());

        verifyNoMoreInteractions(backingStore);
        verify(fileMappingRepository).save(new PersistableFileMapping(persistedFile.getFileId(), PERMANENT, MONGO_GRID_FS, fileIdentifier,
            SHA_256, SHA_512, FILE_SIZE, false));
        PersistedFile expectedPersistedFile =
            new PersistedFile(persistedFile.getFileId(), PERMANENT, MONGO_GRID_FS, fileIdentifier, SHA_256, SHA_512, FILE_SIZE);
        assertEquals(expectedPersistedFile, persistedFile);
    }

    @Test
    void uploadAsStream_WillDeduplicate_WhenFileAlreadyPresentInStore() throws IOException {
        when(backingStore.backingStorageType()).thenReturn(MONGO_GRID_FS);
        when(backingStore.uploadStream(any(InputStream.class), eq(ORIGINAL_FILENAME))).thenAnswer(invocation -> {
            InputStream inputFile = invocation.getArgument(0);
            inputFile.readAllBytes();
            inputFile.close();
            return fileIdentifier;
        });

        when(fileMappingRepository.findAll(any(Example.class))).thenReturn(
            List.of(
                new PersistableFileMapping(randomUUID(), PERMANENT, MONGO_GRID_FS, EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512,
                    FILE_SIZE, false),
                new PersistableFileMapping(randomUUID(), PERMANENT, MONGO_GRID_FS, EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512,
                    FILE_SIZE, false),
                new PersistableFileMapping(randomUUID(), PERMANENT, MONGO_GRID_FS, EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512,
                    FILE_SIZE, false)));

        PersistedFile persistedFile = permanentDeduplicatingFileStore.uploadAsStream(ORIGINAL_FILENAME, createTempFileWithContents());

        verify(backingStore).delete(fileIdentifier);
        verify(fileMappingRepository).save(new PersistableFileMapping(persistedFile.getFileId(), PERMANENT, MONGO_GRID_FS,
            EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512, FILE_SIZE, false));
        PersistedFile expectedPersistedFile = new PersistedFile(persistedFile.getFileId(), PERMANENT, MONGO_GRID_FS,
            EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512, FILE_SIZE);
        assertEquals(expectedPersistedFile, persistedFile);
    }

    @Test
    void uploadAsStreamWithKnownFileSize_WillDeduplicate_WhenFileAlreadyPresentInStore() throws IOException {
        when(backingStore.backingStorageType()).thenReturn(MONGO_GRID_FS);
        when(backingStore.uploadStream(any(InputStream.class), eq(ORIGINAL_FILENAME), eq(FILE_SIZE))).thenAnswer(invocation -> {
            InputStream inputFile = invocation.getArgument(0);
            inputFile.readAllBytes();
            inputFile.close();
            return fileIdentifier;
        });

        when(fileMappingRepository.findAll(any(Example.class))).thenReturn(
            List.of(
                new PersistableFileMapping(randomUUID(), PERMANENT, MONGO_GRID_FS, EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512,
                    FILE_SIZE, false),
                new PersistableFileMapping(randomUUID(), PERMANENT, MONGO_GRID_FS, EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512,
                    FILE_SIZE, false),
                new PersistableFileMapping(randomUUID(), PERMANENT, MONGO_GRID_FS, EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512,
                    FILE_SIZE, false)));

        PersistedFile persistedFile =
            permanentDeduplicatingFileStore.uploadAsStream(ORIGINAL_FILENAME, FILE_SIZE, createTempFileWithContents());

        verify(backingStore).delete(fileIdentifier);
        verify(fileMappingRepository).save(new PersistableFileMapping(persistedFile.getFileId(), PERMANENT, MONGO_GRID_FS,
            EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512, FILE_SIZE, false));
        PersistedFile expectedPersistedFile = new PersistedFile(persistedFile.getFileId(), PERMANENT, MONGO_GRID_FS,
            EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512, FILE_SIZE);
        assertEquals(expectedPersistedFile, persistedFile);
    }

    @Test
    void downloadAsStream_WillReturnInputStreamOfKnownLengthFromFileStore() throws IOException {
        InputStream inputStream = new ByteArrayInputStream(TEMPORARY_FILE_CONTENTS.getBytes());
        when(backingStore.downloadAsStream(EXISTING_BACKING_STORE_FILE_ID))
            .thenReturn(new InputStreamOfKnownLength(inputStream, FILE_SIZE));

        PersistableFileMapping persistableFileMapping = new PersistableFileMapping(randomUUID(), PERMANENT, MONGO_GRID_FS,
            EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512, FILE_SIZE, false);
        InputStreamOfKnownLength inputStreamOfKnownLength = permanentDeduplicatingFileStore.downloadAsStream(persistableFileMapping);

        verify(backingStore).downloadAsStream(EXISTING_BACKING_STORE_FILE_ID);
        assertEquals(inputStreamOfKnownLength.getLength(), FILE_SIZE);
        assertEquals(inputStreamOfKnownLength.getInputStream(), inputStream);
    }

    private InputStream createTempFileWithContents() throws IOException {
        Path tempPath = createTempFile("unit", "test");
        try (var outStream = Files.newOutputStream(tempPath)) {
            outStream.write(TEMPORARY_FILE_CONTENTS.getBytes());
            outStream.flush();
        }
        return new FileInputStream(tempPath.toFile());
    }
}
