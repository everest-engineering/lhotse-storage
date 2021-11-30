package engineering.everest.starterkit.filestorage;

import engineering.everest.starterkit.filestorage.persistence.FileMappingRepository;
import engineering.everest.starterkit.filestorage.persistence.PersistableFileMapping;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.UUID;

import static engineering.everest.starterkit.filestorage.FileStoreType.PERMANENT;
import static java.nio.file.Files.createTempFile;

/**
 * Service layer for working with the permanent and ephemeral file stores.
 */
public class FileService {

    private final FileMappingRepository fileMappingRepository;
    private final PermanentDeduplicatingFileStore permanentDeduplicatingFileStore;
    private final EphemeralDeduplicatingFileStore ephemeralDeduplicatingFileStore;

    public FileService(FileMappingRepository fileMappingRepository,
                       PermanentDeduplicatingFileStore permanentDeduplicatingFileStore,
                       EphemeralDeduplicatingFileStore ephemeralDeduplicatingFileStore) {
        this.fileMappingRepository = fileMappingRepository;
        this.permanentDeduplicatingFileStore = permanentDeduplicatingFileStore;
        this.ephemeralDeduplicatingFileStore = ephemeralDeduplicatingFileStore;
    }

    /**
     * Create a temporary file on the application's local filesystem. Callers should delete this file when it is no longer required.
     *
     * @param  suffix      to append to the temporary file name. Can be null.
     * @return             a file
     * @throws IOException if the file could not be created
     */
    public File createTemporaryFile(String suffix) throws IOException {
        File tempFile = createTempFile("temp", suffix).toFile();
        tempFile.deleteOnExit();
        return tempFile;
    }

    /**
     * Streaming upload of a named file to the permanent file store. File length is derived from reading the input stream.
     *
     * @param  originalFilename to record for the file
     * @param  inputStream      to read from. Must be closed by the caller.
     * @return                  UUID assigned to this file.
     */
    public UUID transferToPermanentStore(String originalFilename, InputStream inputStream) throws IOException {
        return permanentDeduplicatingFileStore.uploadAsStream(originalFilename, inputStream).getPersistedFileIdentifier().getFileId();
    }

    /**
     * Streaming upload of a named file to the permanent file store. File length is provided by the caller.
     *
     * @param  originalFilename to record for the file
     * @param  inputStream      to read from. Must be closed by the caller.
     * @return                  UUID assigned to this file.
     */
    public UUID transferToPermanentStore(String originalFilename, long fileSize, InputStream inputStream) throws IOException {
        return permanentDeduplicatingFileStore.uploadAsStream(originalFilename, fileSize, inputStream).getPersistedFileIdentifier()
            .getFileId();
    }

    /**
     * Streaming upload of a named file to the ephemeral file store. File length is derived from reading the input stream.
     *
     * @param  filename    to record for the file
     * @param  inputStream to read from. Must be closed by the caller.
     * @return             UUID assigned to this file.
     */
    public UUID transferToEphemeralStore(String filename, InputStream inputStream) throws IOException {
        return ephemeralDeduplicatingFileStore.uploadAsStream(filename, inputStream).getPersistedFileIdentifier().getFileId();
    }

    /**
     * Streaming upload of an unnamed file to the ephemeral file store. File length is derived from reading the input stream.
     *
     * @param  inputStream to read from. Must be closed by the caller.
     * @return             UUID assigned to this file.
     */
    public UUID transferToEphemeralStore(InputStream inputStream) throws IOException {
        return transferToEphemeralStore("", inputStream);
    }

    /**
     * Streaming download of file.
     *
     * @param  fileId      is the UUID originally assigned to the file.
     * @return             input stream of known length
     * @throws IOException if the file cannot be read
     */
    public InputStreamOfKnownLength stream(UUID fileId) throws IOException {
        PersistableFileMapping persistableFileMapping = fileMappingRepository.findById(fileId).orElseThrow();
        var fileStore = persistableFileMapping.getFileStoreType().equals(PERMANENT)
            ? permanentDeduplicatingFileStore
            : ephemeralDeduplicatingFileStore;
        return fileStore.downloadAsStream(persistableFileMapping);
    }

    /**
     * Marking ephemeral files for deletion
     *
     * @param  persistedFileIdentifiers to mark for deletion
     * @throws IllegalArgumentException if the file is not ephemeral
     */
    public void markFilesForDeletion(Set<PersistedFileIdentifier> persistedFileIdentifiers) {
        ephemeralDeduplicatingFileStore.markFilesForDeletion(persistedFileIdentifiers);
    }

    /**
     * Mark an ephemeral file for deletion
     *
     * @param  persistedFileIdentifier  to mark for deletion
     * @throws IllegalArgumentException if the file is not ephemeral
     */
    public void markFileForDeletion(PersistedFileIdentifier persistedFileIdentifier) {
        ephemeralDeduplicatingFileStore.markFileForDeletion(persistedFileIdentifier);
    }

    /**
     * Marks all ephemeral files for deletion
     */
    public void markAllFilesForDeletion() {
        ephemeralDeduplicatingFileStore.markAllFilesForDeletion();
    }

    /**
     * Delete ephemeral files in a batch
     *
     * @param batchSize is the number of files to delete
     */
    public void deleteFileBatch(int batchSize) {
        ephemeralDeduplicatingFileStore.deleteFileBatch(batchSize);
    }
}
