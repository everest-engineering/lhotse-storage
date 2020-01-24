package engineering.everest.starterkit.filestorage;

import engineering.everest.starterkit.filestorage.persistence.FileMappingRepository;
import engineering.everest.starterkit.filestorage.persistence.PersistableFileMapping;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import static java.nio.file.Files.createTempFile;

@Component
public class FileService {

    private final FileMappingRepository fileMappingRepository;
    @Qualifier("permanentDeduplicatingFileStore")
    private final DeduplicatingFileStore permanentFileStore;
    @Qualifier("ephemeralDeduplicatingFileStore")
    private final DeduplicatingFileStore ephemeralFileStore;

    public FileService(FileMappingRepository fileMappingRepository,
                       DeduplicatingFileStore permanentFileStore,
                       DeduplicatingFileStore ephemeralFileStore) {
        this.fileMappingRepository = fileMappingRepository;
        this.permanentFileStore = permanentFileStore;
        this.ephemeralFileStore = ephemeralFileStore;
    }

    public File createTemporaryFile() throws IOException {
        File tempFile = createTempFile("temp", "upload").toFile();
        tempFile.deleteOnExit();
        return tempFile;
    }

    public UUID transferToPermanentStore(String originalFilename, InputStream inputStream) throws IOException {
        return permanentFileStore.store(originalFilename, inputStream).getPersistedFileIdentifier().getFileId();
    }

    public UUID transferToEphemeralStore(String filename, InputStream inputStream) throws IOException {
        return ephemeralFileStore.store(filename, inputStream).getPersistedFileIdentifier().getFileId();
    }

    public UUID transferToEphemeralStore(InputStream inputStream) throws IOException {
        return transferToEphemeralStore("", inputStream);
    }

    public InputStream stream(UUID fileId) throws IOException {
        PersistableFileMapping persistableFileMapping = fileMappingRepository.findById(fileId).orElseThrow();
        var fileStore = persistableFileMapping.getFileStoreType().equals(FileStoreType.PERMANENT) ? permanentFileStore : ephemeralFileStore;
        return fileStore.stream(persistableFileMapping.getPersistedFileIdentifier());
    }
}
