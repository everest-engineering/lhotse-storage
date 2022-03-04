package engineering.everest.starterkit.filestorage;

import engineering.everest.starterkit.filestorage.persistence.FileMappingRepository;
import engineering.everest.starterkit.filestorage.persistence.PersistableFileMapping;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static engineering.everest.starterkit.filestorage.BackingStorageType.AWS_S3;
import static engineering.everest.starterkit.filestorage.FileStoreType.EPHEMERAL;
import static engineering.everest.starterkit.filestorage.FileStoreType.PERMANENT;
import static engineering.everest.starterkit.filestorage.BackingStorageType.MONGO_GRID_FS;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FileServiceTest {

    private static final String ORIGINAL_FILENAME = "original-filename";

    private static final int BATCH_SIZE = 50;

    private FileService fileService;

    @Mock
    private FileMappingRepository fileMappingRepository;
    @Mock
    private PermanentDeduplicatingFileStore permanentFileStore;
    @Mock
    private EphemeralDeduplicatingFileStore ephemeralFileStore;

    @BeforeEach
    void setUp() {
        fileService = new FileService(fileMappingRepository, permanentFileStore, ephemeralFileStore);
    }

    @Test
    void createTempFile_WillCreateATemporaryFileMarkedAsDeleteOnExit() throws IOException {
        File temporaryFile = fileService.createTemporaryFile("upload");

        assertTrue(temporaryFile.canWrite());
        assertTrue(temporaryFile.canRead());
    }

    @Test
    void transferToPermanentStore_WillDelegateToPermanentStore() throws IOException {
        when(permanentFileStore.uploadAsStream(eq(ORIGINAL_FILENAME), any(InputStream.class))).thenReturn(new PersistedFile());

        File tempFile = fileService.createTemporaryFile("upload");
        try (FileInputStream inputStream = new FileInputStream(tempFile)) {
            fileService.transferToPermanentStore(ORIGINAL_FILENAME, inputStream);
            verify(permanentFileStore).uploadAsStream(ORIGINAL_FILENAME, inputStream);
        }

        verifyNoInteractions(ephemeralFileStore);
    }

    @Test
    void transferToPermanentStoreWithFileSize_WillDelegateToPermanentStore() throws IOException {
        when(permanentFileStore.uploadAsStream(eq(ORIGINAL_FILENAME), eq(42L), any(InputStream.class))).thenReturn(new PersistedFile());

        File tempFile = fileService.createTemporaryFile("upload");
        try (FileInputStream inputStream = new FileInputStream(tempFile)) {
            fileService.transferToPermanentStore(ORIGINAL_FILENAME, 42L, inputStream);
            verify(permanentFileStore).uploadAsStream(ORIGINAL_FILENAME, 42L, inputStream);
        }

        verifyNoInteractions(ephemeralFileStore);
    }

    @Test
    void transferToEphemeralStore_WillDelegateToEphemeralStore() throws IOException {
        when(ephemeralFileStore.uploadAsStream(eq(ORIGINAL_FILENAME), any(InputStream.class))).thenReturn(new PersistedFile());

        File tempFile = fileService.createTemporaryFile("upload");
        try (FileInputStream inputStream = new FileInputStream(tempFile)) {
            fileService.transferToEphemeralStore(ORIGINAL_FILENAME, inputStream);
            verify(ephemeralFileStore).uploadAsStream(ORIGINAL_FILENAME, inputStream);
        }

        verifyNoInteractions(permanentFileStore);
    }

    @Test
    void transferToEphemeralStore_WillDelegateToEphemeralStore_WhenNoFilenamespecified() throws IOException {
        when(ephemeralFileStore.uploadAsStream(eq(""), any(InputStream.class))).thenReturn(new PersistedFile());

        File tempFile = fileService.createTemporaryFile("upload");
        try (FileInputStream inputStream = new FileInputStream(tempFile)) {
            fileService.transferToEphemeralStore(inputStream);
            verify(ephemeralFileStore).uploadAsStream("", inputStream);
        }

        verifyNoInteractions(permanentFileStore);
    }

    @Test
    void fileSizeInBytes_WillReturnSizePersistedInFileMapping() {
        UUID fileId = randomUUID();
        var persistedFileIdentifier = new PersistedFileIdentifier(fileId, PERMANENT, AWS_S3, "native-file-id");
        var persistableFileMapping = new PersistableFileMapping(fileId, PERMANENT, AWS_S3, "native-file-id",
            "", "", 87654321L, false);
        when(fileMappingRepository.findById(persistedFileIdentifier.getFileId())).thenReturn(Optional.of(persistableFileMapping));

        assertEquals(87654321L, fileService.fileSizeInBytes(fileId));
    }

    @Test
    void stream_WillDelegateToPermanentFileStore_WhenFileMapsToPermanentStore() throws IOException {
        UUID fileId = randomUUID();
        var persistedFileIdentifier = new PersistedFileIdentifier(fileId, PERMANENT, MONGO_GRID_FS, "native-file-id");
        var persistableFileMapping = new PersistableFileMapping(fileId, PERMANENT, MONGO_GRID_FS, "native-file-id",
            "", "", 123L, false);
        var inputStreamOngoingStubbing = new ByteArrayInputStream("hello".getBytes());

        when(fileMappingRepository.findById(persistedFileIdentifier.getFileId())).thenReturn(Optional.of(persistableFileMapping));
        when(permanentFileStore.downloadAsStream(persistableFileMapping, 0L))
            .thenReturn(new InputStreamOfKnownLength(inputStreamOngoingStubbing, 10L));

        assertEquals(new InputStreamOfKnownLength(inputStreamOngoingStubbing, 10L),
            fileService.stream(persistedFileIdentifier.getFileId()));
    }

    @Test
    void stream_WillDelegateToEphemeralFileStore_WhenFileMapsToEphemeralStore() throws IOException {
        UUID fileId = randomUUID();
        var persistedFileIdentifier = new PersistedFileIdentifier(fileId, EPHEMERAL, MONGO_GRID_FS, "native-file-id");
        var persistableFileMapping = new PersistableFileMapping(fileId, EPHEMERAL, MONGO_GRID_FS, "native-file-id",
            "", "", 123L, false);
        var inputStreamOngoingStubbing = new ByteArrayInputStream("hello".getBytes());

        when(fileMappingRepository.findById(persistedFileIdentifier.getFileId())).thenReturn(Optional.of(persistableFileMapping));
        when(ephemeralFileStore.downloadAsStream(persistableFileMapping, 0L))
            .thenReturn(new InputStreamOfKnownLength(inputStreamOngoingStubbing, 10L));

        assertEquals(new InputStreamOfKnownLength(inputStreamOngoingStubbing, 10L),
            fileService.stream(persistedFileIdentifier.getFileId()));
    }

    @Test
    void markFileForDeletion_WillDelegateToEphemeralFileStore() {
        UUID fileId = randomUUID();
        var persistedFileIdentifier = new PersistedFileIdentifier(fileId, EPHEMERAL, MONGO_GRID_FS, "native-file-id");

        fileService.markEphemeralFileForDeletion(persistedFileIdentifier);

        verify(ephemeralFileStore).markFileForDeletion(persistedFileIdentifier);
    }

    @Test
    void markFilesForDeletion_WillDelegateToEphemeralFileStore() {
        UUID fileId = randomUUID();
        var persistedFileIdentifier = new PersistedFileIdentifier(fileId, EPHEMERAL, MONGO_GRID_FS, "native-file-id");
        Set<PersistedFileIdentifier> persistedFileIdentifier1 = Set.of(persistedFileIdentifier);

        fileService.markEphemeralFilesForDeletion(persistedFileIdentifier1);

        verify(ephemeralFileStore).markFilesForDeletion(persistedFileIdentifier1);
    }

    @Test
    void markAllFilesForDeletion_WillDelegateToEphemeralFileStore() {
        fileService.markAllEphemeralFilesForDeletion();

        verify(ephemeralFileStore).markAllFilesForDeletion();
    }

    @Test
    void deleteFileBatch_WillDelegateToEphemeralFileStore() {
        fileService.deleteEphemeralFileBatch(BATCH_SIZE);

        verify(ephemeralFileStore).deleteFileBatch(BATCH_SIZE);
    }
}
