package engineering.everest.starterkit.filestorage.filestores;

import engineering.everest.starterkit.filestorage.InputStreamOfKnownLength;
import engineering.everest.starterkit.filestorage.PersistedFileIdentifier;
import engineering.everest.starterkit.filestorage.backing.BackingStore;
import engineering.everest.starterkit.filestorage.persistence.FileMappingRepository;
import engineering.everest.starterkit.filestorage.persistence.PersistableFileMapping;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static engineering.everest.starterkit.filestorage.backing.BackingStorageType.MONGO_GRID_FS;
import static engineering.everest.starterkit.filestorage.filestores.FileStoreType.EPHEMERAL;
import static engineering.everest.starterkit.filestorage.filestores.FileStoreType.PERMANENT;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EphemeralDeduplicatingFileStoreTest {

    private static final String EXISTING_BACKING_STORE_FILE_ID = "existing-backing-store-file-id";
    private static final String SHA_256 = "108e0047119fdf8db72dc146283d0cd717d620a9b4fb9ead902e22f4c04fbe7b";
    private static final String SHA_512 =
        "cb61c18674f50eedd4f7d77f938b11d468713516b14862c4ae4ea68ec5aa30c1475d7d38f17e14585da10ea848a054733f2185b1ea57f10a1c416bb1617baa60";
    private static final String TEMPORARY_FILE_CONTENTS = "A temporary file for unit testing";
    private static final Long FILE_SIZE = (long) TEMPORARY_FILE_CONTENTS.length();
    private static final UUID FILE_ID_1 = randomUUID();
    private static final UUID FILE_ID_2 = randomUUID();
    private static final int BATCH_SIZE = 50;

    private EphemeralDeduplicatingFileStore ephemeralDeduplicatingFileStore;

    @Mock
    protected BackingStore backingStore;
    @Mock
    protected FileMappingRepository fileMappingRepository;

    @BeforeEach
    void setUp() {
        ephemeralDeduplicatingFileStore = new EphemeralDeduplicatingFileStore(fileMappingRepository, backingStore);
    }

    @Test
    void markFileForDeletion_WillMarkFileForDeletionInFileMappingRepository() {
        var uuid = randomUUID();
        var uuid2 = randomUUID();
        when(fileMappingRepository.findById(uuid)).thenReturn(of(new PersistableFileMapping(uuid2, EPHEMERAL, MONGO_GRID_FS,
            EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512, FILE_SIZE, false)));

        var persistedFileIdentifier = new PersistedFileIdentifier(uuid, EPHEMERAL, MONGO_GRID_FS, EXISTING_BACKING_STORE_FILE_ID);

        ephemeralDeduplicatingFileStore.markFileForDeletion(persistedFileIdentifier);

        verifyNoInteractions(backingStore);
        verify(fileMappingRepository).save(
            new PersistableFileMapping(uuid2, EPHEMERAL, MONGO_GRID_FS, EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512, FILE_SIZE, true));
    }

    @Test
    void markFileForDeletion_WillDoNothingIfFileMappingDoesNotExist() {
        var fileId = randomUUID();
        when(fileMappingRepository.findById(fileId)).thenReturn(Optional.empty());

        var persistedFileIdentifier = new PersistedFileIdentifier(fileId, EPHEMERAL, MONGO_GRID_FS, EXISTING_BACKING_STORE_FILE_ID);

        ephemeralDeduplicatingFileStore.markFileForDeletion(persistedFileIdentifier);

        verifyNoInteractions(backingStore);
        verifyNoMoreInteractions(fileMappingRepository);
    }

    @Test
    void markFilesForDeletion_WillMarkFilesForDeletionInFileMappingRepository() {
        var fileId = randomUUID();
        when(fileMappingRepository.findById(fileId)).thenReturn(of(new PersistableFileMapping(fileId, EPHEMERAL, MONGO_GRID_FS,
            EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512, FILE_SIZE, false)));

        var persistedFileIdentifier = new PersistedFileIdentifier(fileId, EPHEMERAL, MONGO_GRID_FS, EXISTING_BACKING_STORE_FILE_ID);

        ephemeralDeduplicatingFileStore.markFilesForDeletion(List.of(persistedFileIdentifier));

        verifyNoInteractions(backingStore);
        verify(fileMappingRepository).save(
            new PersistableFileMapping(fileId, EPHEMERAL, MONGO_GRID_FS, EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512, FILE_SIZE,
                true));
    }

    @Test
    void markFilesForDeletion_WillFail_WhenFileIsNotEphemeral() {
        var persistedFileIdentifier = new PersistedFileIdentifier(randomUUID(), PERMANENT, MONGO_GRID_FS, EXISTING_BACKING_STORE_FILE_ID);

        assertThrows(IllegalArgumentException.class,
            () -> ephemeralDeduplicatingFileStore.markFilesForDeletion(List.of(persistedFileIdentifier)));
    }

    @Test
    void markAllFilesForDeletion_WillMarkAllFilesInFileMappingRepositoryForDeletion() {
        var persistableFileMapping = new PersistableFileMapping(FILE_ID_1, EPHEMERAL, MONGO_GRID_FS,
            EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512, FILE_SIZE, false);
        when(fileMappingRepository.findByFileStoreType(EPHEMERAL)).thenReturn(List.of(persistableFileMapping));

        ephemeralDeduplicatingFileStore.markAllFilesForDeletion();

        assertTrue(persistableFileMapping.isMarkedForDeletion());
        verify(fileMappingRepository).saveAll(List.of(persistableFileMapping));
    }

    @Test
    void deleteBatchOfFilesMarkedForDeletion_WillDeleteFilesFromFileStoreAndFromFileMappingRepository() {
        var persistableFileMapping = new PersistableFileMapping(FILE_ID_1, EPHEMERAL, MONGO_GRID_FS,
            EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512, FILE_SIZE, false);
        when(fileMappingRepository.findByMarkedForDeletionTrue(PageRequest.of(0, BATCH_SIZE)))
            .thenReturn(List.of(persistableFileMapping));
        when(fileMappingRepository.findByBackingStorageFileId(EXISTING_BACKING_STORE_FILE_ID))
            .thenReturn(List.of(persistableFileMapping));

        ephemeralDeduplicatingFileStore.deleteBatchOfFilesMarkedForDeletion(BATCH_SIZE);

        verify(fileMappingRepository).deleteAll(List.of(persistableFileMapping));
        verify(backingStore).deleteFiles(Set.of(EXISTING_BACKING_STORE_FILE_ID));
    }

    @Test
    void deleteBatchOfFilesMarkedForDeletion_WillOnlyUnlinkFileInFileMappingRepository_WhenBackingFileIsStillReferenced() {
        var persistableFileMappingToDelete = new PersistableFileMapping(FILE_ID_1, EPHEMERAL, MONGO_GRID_FS,
            EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512, FILE_SIZE, true);
        var persistableFileMappingToRetain = new PersistableFileMapping(FILE_ID_2, EPHEMERAL, MONGO_GRID_FS,
            EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512, FILE_SIZE, false);

        when(fileMappingRepository.findByMarkedForDeletionTrue(PageRequest.of(0, BATCH_SIZE)))
            .thenReturn(List.of(persistableFileMappingToDelete));
        when(fileMappingRepository.findByBackingStorageFileId(EXISTING_BACKING_STORE_FILE_ID))
            .thenReturn(List.of(persistableFileMappingToDelete, persistableFileMappingToRetain));

        ephemeralDeduplicatingFileStore.deleteBatchOfFilesMarkedForDeletion(BATCH_SIZE);

        verify(fileMappingRepository).deleteAll(List.of(persistableFileMappingToDelete));
        verify(fileMappingRepository, never()).deleteById(FILE_ID_2);
        verify(backingStore, never()).deleteFiles(any());
    }

    @Test
    void deleteBatchOfFilesMarkedForDeletion_WillRecoverIfInterruptedDuringBackingStorageFileDeletion() {
        var persistableFileMappingToDelete1 = new PersistableFileMapping(FILE_ID_1, EPHEMERAL, MONGO_GRID_FS,
            EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512, FILE_SIZE, true);
        var persistableFileMappingToDelete2 = new PersistableFileMapping(FILE_ID_2, EPHEMERAL, MONGO_GRID_FS,
            EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512, FILE_SIZE, true);
        when(fileMappingRepository.findByMarkedForDeletionTrue(PageRequest.of(0, BATCH_SIZE)))
            .thenReturn(List.of(persistableFileMappingToDelete1, persistableFileMappingToDelete2));
        when(fileMappingRepository.findByBackingStorageFileId(EXISTING_BACKING_STORE_FILE_ID))
            .thenReturn(List.of(persistableFileMappingToDelete2));

        ephemeralDeduplicatingFileStore.deleteBatchOfFilesMarkedForDeletion(BATCH_SIZE);

        verify(fileMappingRepository).deleteAll(List.of(persistableFileMappingToDelete1, persistableFileMappingToDelete2));
        verify(backingStore).deleteFiles(Set.of(EXISTING_BACKING_STORE_FILE_ID));
    }

    @Test
    void downloadAsStream_WillReturnExceptionIfFileAlreadyMarkedForDeletion() {
        var persistableFileMapping = new PersistableFileMapping(randomUUID(), EPHEMERAL, MONGO_GRID_FS,
            EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512, FILE_SIZE, true);

        assertThrows(NoSuchElementException.class, () -> ephemeralDeduplicatingFileStore.downloadAsStream(persistableFileMapping));
    }

    @Test
    void downloadAsStream_WillReturnInputStreamOfKnownLengthFromFileStore() throws IOException {
        var inputStream = new ByteArrayInputStream(TEMPORARY_FILE_CONTENTS.getBytes());
        when(backingStore.downloadAsStream(EXISTING_BACKING_STORE_FILE_ID))
            .thenReturn(new InputStreamOfKnownLength(inputStream, FILE_SIZE));

        var persistableFileMapping = new PersistableFileMapping(randomUUID(), PERMANENT, MONGO_GRID_FS,
            EXISTING_BACKING_STORE_FILE_ID, SHA_256, SHA_512, FILE_SIZE, false);
        var inputStreamOfKnownLength = ephemeralDeduplicatingFileStore.downloadAsStream(persistableFileMapping);

        assertEquals(inputStreamOfKnownLength.getLength(), FILE_SIZE);
        assertEquals(inputStreamOfKnownLength.getInputStream(), inputStream);
    }
}
