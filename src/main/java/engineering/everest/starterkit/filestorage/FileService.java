package engineering.everest.starterkit.filestorage;

import engineering.everest.starterkit.filestorage.filestores.EphemeralDeduplicatingFileStore;
import engineering.everest.starterkit.filestorage.filestores.PermanentDeduplicatingFileStore;
import engineering.everest.starterkit.filestorage.persistence.FileMappingRepository;
import engineering.everest.starterkit.filestorage.persistence.PersistableFileMapping;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.UUID;

import static engineering.everest.starterkit.filestorage.filestores.FileStoreType.PERMANENT;
import static java.nio.file.Files.createTempFile;
import static java.util.stream.Collectors.toList;

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
     * @throws IOException      if the file could not be persisted
     */
    public UUID transferToPermanentStore(String originalFilename, InputStream inputStream) throws IOException {
        return permanentDeduplicatingFileStore.uploadAsStream(originalFilename, inputStream).getPersistedFileIdentifier().getFileId();
    }

    /**
     * Streaming upload of a named file to the permanent file store. File length is provided by the caller. <b>This method should be
     * preferred when using the AWS S3 client.</b>
     *
     * @param  originalFilename to record for the file
     * @param  fileSize         size of the file
     * @param  inputStream      to read from. Must be closed by the caller.
     * @return                  UUID assigned to this file.
     * @throws IOException      if the file could not be persisted
     */
    public UUID transferToPermanentStore(String originalFilename, long fileSize, InputStream inputStream) throws IOException {
        return permanentDeduplicatingFileStore.uploadAsStream(originalFilename, fileSize, inputStream).getPersistedFileIdentifier()
            .getFileId();
    }

    /**
     * Streaming upload of an unnamed file to the ephemeral file store. File length is derived from reading the input stream.
     *
     * @param  inputStream to read from. Must be closed by the caller.
     * @return             UUID assigned to this file.
     * @throws IOException if the file could not be persisted
     */
    public UUID transferToEphemeralStore(InputStream inputStream) throws IOException {
        return transferToEphemeralStore("", inputStream);
    }

    /**
     * Streaming upload of a named file to the ephemeral file store. File length is derived from reading the input stream.
     *
     * @param  filename    to record for the file
     * @param  inputStream to read from. Must be closed by the caller.
     * @return             UUID assigned to this file.
     * @throws IOException if the file could not be persisted
     */
    public UUID transferToEphemeralStore(String filename, InputStream inputStream) throws IOException {
        return ephemeralDeduplicatingFileStore.uploadAsStream(filename, inputStream).getPersistedFileIdentifier().getFileId();
    }

    /**
     * Streaming upload of a named file to the ephemeral file store. File length is derived from reading the input stream. <b>This method
     * should be preferred when using the AWS S3 client.</b>
     *
     * @param  filename    to record for the file
     * @param  fileSize    size of the file
     * @param  inputStream to read from. Must be closed by the caller.
     * @return             UUID assigned to this file.
     * @throws IOException if the file could not be persisted
     */
    public UUID transferToEphemeralStore(String filename, long fileSize, InputStream inputStream) throws IOException {
        return ephemeralDeduplicatingFileStore.uploadAsStream(filename, fileSize, inputStream).getPersistedFileIdentifier().getFileId();
    }

    /**
     * Size of a file
     *
     * @param  fileId is the UUID originally assigned to the file.
     * @return        size in bytes
     */
    public long fileSizeInBytes(UUID fileId) {
        return fileMappingRepository.findById(fileId).orElseThrow().getFileSizeBytes();
    }

    /**
     * Streaming download of file
     *
     * @param  fileId      is the UUID originally assigned to the file.
     * @return             input stream of known length
     * @throws IOException if the file cannot be read
     */
    public InputStreamOfKnownLength stream(UUID fileId) throws IOException {
        return stream(fileId, 0L);
    }

    /**
     * Streaming download of file starting at a given offset
     *
     * @param  fileId         is the UUID originally assigned to the file.
     * @param  startingOffset binary offset into the file from which to start streaming from
     * @return                input stream of known length
     * @throws IOException    if the file cannot be read
     */
    public InputStreamOfKnownLength stream(UUID fileId, long startingOffset) throws IOException {
        return stream(fileId, startingOffset, fileSizeInBytes(fileId) - 1);
    }

    /**
     * Streaming download of file starting at a given offset
     *
     * @param  fileId         is the UUID originally assigned to the file.
     * @param  startingOffset binary offset into the file from which to start streaming from
     * @param  endingOffset   binary offset into the file to stream to (inclusive)
     * @return                input stream of known length
     * @throws IOException    if the file cannot be read
     */
    public InputStreamOfKnownLength stream(UUID fileId, long startingOffset, long endingOffset) throws IOException {
        var persistableFileMapping = fileMappingRepository.findById(fileId).orElseThrow();
        var fileStore = persistableFileMapping.getFileStoreType().equals(PERMANENT)
            ? permanentDeduplicatingFileStore
            : ephemeralDeduplicatingFileStore;
        return fileStore.downloadAsStream(persistableFileMapping, startingOffset, endingOffset);
    }

    /**
     * Marking ephemeral files for deletion
     *
     * @param  fileIds                  to mark for deletion
     * @throws IllegalArgumentException if any file is not ephemeral
     */
    public void markEphemeralFilesForDeletion(Set<UUID> fileIds) {
        var persistedFileIdentifiers = fileMappingRepository.findAllById(fileIds).stream()
            .map(PersistableFileMapping::getPersistedFileIdentifier)
            .collect(toList());
        ephemeralDeduplicatingFileStore.markFilesForDeletion(persistedFileIdentifiers);
    }

    /**
     * Mark an ephemeral file for deletion
     *
     * @param  fileId                   to mark for deletion
     * @throws IllegalArgumentException if the file is not ephemeral
     */
    public void markEphemeralFileForDeletion(UUID fileId) {
        fileMappingRepository.findById(fileId).ifPresent(persistableFileMapping -> ephemeralDeduplicatingFileStore
            .markFileForDeletion(persistableFileMapping.getPersistedFileIdentifier()));
    }

    /**
     * Marks all ephemeral files for deletion
     */
    public void markAllEphemeralFilesForDeletion() {
        ephemeralDeduplicatingFileStore.markAllFilesForDeletion();
    }

    /**
     * Delete ephemeral files in a batch
     *
     * @param batchSize is the number of files to delete
     */
    public void deleteEphemeralFileBatch(int batchSize) {
        ephemeralDeduplicatingFileStore.deleteFileBatch(batchSize);
    }
}
