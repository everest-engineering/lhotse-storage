package engineering.everest.starterkit.filestorage;

import engineering.everest.starterkit.filestorage.persistence.FileMappingRepository;
import engineering.everest.starterkit.filestorage.persistence.PersistableFileMapping;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import static java.nio.file.Files.createTempFile;

public class FileService {

    private final FileMappingRepository fileMappingRepository;
    private final DeduplicatingFileStore permanentFileStore;
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
        return permanentFileStore.uploadAsStream(originalFilename, inputStream).getPersistedFileIdentifier().getFileId();
    }

    public UUID transferToPermanentStore(String originalFilename, long fileSize, InputStream inputStream) throws IOException {
        return permanentFileStore.uploadAsStream(originalFilename, fileSize, inputStream).getPersistedFileIdentifier().getFileId();
    }

    public UUID transferToEphemeralStore(String filename, InputStream inputStream) throws IOException {
        return ephemeralFileStore.uploadAsStream(filename, inputStream).getPersistedFileIdentifier().getFileId();
    }

    public UUID transferToEphemeralStore(InputStream inputStream) throws IOException {
        return transferToEphemeralStore("", inputStream);
    }

    public InputStreamOfKnownLength stream(UUID fileId) throws IOException {
        PersistableFileMapping persistableFileMapping = fileMappingRepository.findById(fileId).orElseThrow();
        var fileStore = persistableFileMapping.getFileStoreType().equals(FileStoreType.PERMANENT) ? permanentFileStore : ephemeralFileStore;
        return fileStore.downloadAsStream(persistableFileMapping.getPersistedFileIdentifier());
    }
}
