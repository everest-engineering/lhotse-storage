package engineering.everest.starterkit.filestorage;

import engineering.everest.starterkit.filestorage.persistence.FileMappingRepository;
import engineering.everest.starterkit.filestorage.persistence.PersistableFileMapping;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static engineering.everest.starterkit.filestorage.FileStoreType.EPHEMERAL;
import static engineering.everest.starterkit.filestorage.NativeStorageType.MONGO_GRID_FS;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EphemeralDeduplicatingFileStoreTest {

    private static final String EXISTING_NATIVE_STORE_FILE_ID = "existing-native-store-file-id";
    private static final String SHA_256 = "108e0047119fdf8db72dc146283d0cd717d620a9b4fb9ead902e22f4c04fbe7b";
    private static final String SHA_512 = "cb61c18674f50eedd4f7d77f938b11d468713516b14862c4ae4ea68ec5aa30c1475d7d38f17e14585da10ea848a054733f2185b1ea57f10a1c416bb1617baa60";
    private static final String TEMPORARY_FILE_CONTENTS = "A temporary file for unit testing";
    private static final Long FILE_SIZE = (long) TEMPORARY_FILE_CONTENTS.length();

    private EphemeralDeduplicatingFileStore ephemeralDeduplicatingFileStore;

    @Mock
    protected FileStore fileStore;
    @Mock
    protected FileMappingRepository fileMappingRepository;

    @BeforeEach
    void setUp() {
        ephemeralDeduplicatingFileStore = new EphemeralDeduplicatingFileStore(fileMappingRepository, fileStore);
    }

    @Test
    void deleteFile_WillMarkFileForDeletionInFileMappingRepository() throws IOException {
        UUID uuid = randomUUID();
        UUID uuid2 = randomUUID();
        when(fileMappingRepository.findById(uuid)).thenReturn(of(new PersistableFileMapping(uuid2, EPHEMERAL, MONGO_GRID_FS, EXISTING_NATIVE_STORE_FILE_ID, SHA_256, SHA_512, FILE_SIZE, false)));

        PersistedFileIdentifier persistedFileIdentifier = new PersistedFileIdentifier(uuid, EPHEMERAL, MONGO_GRID_FS, EXISTING_NATIVE_STORE_FILE_ID);

        ephemeralDeduplicatingFileStore.deleteFile(persistedFileIdentifier);

        verifyNoInteractions(fileStore);
        verify(fileMappingRepository).save(new PersistableFileMapping(uuid2, EPHEMERAL, MONGO_GRID_FS, EXISTING_NATIVE_STORE_FILE_ID, SHA_256, SHA_512, FILE_SIZE, true));
    }

    @Test
    void deleteFile_WillDoNothingIfFileMappingDoesNotExist() throws IOException {
        UUID fileId = randomUUID();
        when(fileMappingRepository.findById(fileId)).thenReturn(Optional.empty());

        PersistedFileIdentifier persistedFileIdentifier = new PersistedFileIdentifier(fileId, EPHEMERAL, MONGO_GRID_FS, EXISTING_NATIVE_STORE_FILE_ID);

        ephemeralDeduplicatingFileStore.deleteFile(persistedFileIdentifier);

        verifyNoInteractions(fileStore);
        verifyNoMoreInteractions(fileMappingRepository);
    }

    @Test
    void deleteFiles_WillMarkFilesForDeletionInFileMappingRepository() throws IOException {
        UUID fileId = randomUUID();
        when(fileMappingRepository.findById(fileId)).thenReturn(of(new PersistableFileMapping(fileId, EPHEMERAL, MONGO_GRID_FS, EXISTING_NATIVE_STORE_FILE_ID, SHA_256, SHA_512, FILE_SIZE, false)));

        PersistedFileIdentifier persistedFileIdentifier = new PersistedFileIdentifier(fileId, EPHEMERAL, MONGO_GRID_FS, EXISTING_NATIVE_STORE_FILE_ID);

        ephemeralDeduplicatingFileStore.deleteFiles(Set.of(persistedFileIdentifier));

        verifyNoInteractions(fileStore);
        verify(fileMappingRepository).save(new PersistableFileMapping(fileId, EPHEMERAL, MONGO_GRID_FS, EXISTING_NATIVE_STORE_FILE_ID, SHA_256, SHA_512, FILE_SIZE, true));

    }
}
