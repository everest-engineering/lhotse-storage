package engineering.everest.starterkit.filestorage;

import com.google.common.hash.Hashing;
import com.google.common.hash.HashingInputStream;
import com.google.common.io.CountingInputStream;
import engineering.everest.starterkit.filestorage.persistence.FileMappingRepository;
import engineering.everest.starterkit.filestorage.persistence.PersistableFileMapping;
import org.springframework.data.domain.Example;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static engineering.everest.starterkit.filestorage.FileStoreType.PERMANENT;
import static java.util.UUID.randomUUID;

/**
 * File store that removes duplicate copies of files and manages the mapping of individual file uploads
 * to a single backing file.
 * <p>
 * This default implementation does not support file deletion. It is intended to be used as a permanent file store.
 *
 * @see EphemeralDeduplicatingFileStore
 */
public class PermanentDeduplicatingFileStore {

    protected final FileStoreType fileStoreType;
    protected final FileMappingRepository fileMappingRepository;
    protected final FileStore fileStore;

    public PermanentDeduplicatingFileStore(FileMappingRepository fileMappingRepository,
                                           FileStore fileStore) {
        this(PERMANENT, fileMappingRepository, fileStore);
    }

    protected PermanentDeduplicatingFileStore(FileStoreType fileStoreType,
                                              FileMappingRepository fileMappingRepository,
                                              FileStore fileStore) {
        this.fileStoreType = fileStoreType;
        this.fileMappingRepository = fileMappingRepository;
        this.fileStore = fileStore;
    }

    /**
     * Stream a file of unknown length to the file store, recording its name.
     * <p>
     * <b>If possible, prefer to call the overloaded method that includes the length of the file. Depending
     * on the backing file store implementation, this method may introduce performance overheads.</b>
     *
     * @param originalFilename to record. Typically the original filename a user would associate with the file contents.
     * @param inputStream      containing content to upload. Managed by the caller.
     * @return persisted file information
     */
    public PersistedFile uploadAsStream(String originalFilename, InputStream inputStream) throws IOException {
        try (var countingInputStream = new CountingInputStream(inputStream);
             var sha256ingInputStream = new HashingInputStream(Hashing.sha256(), countingInputStream);
             var sha512ingInputStream = new HashingInputStream(Hashing.sha512(), sha256ingInputStream)) {
            var fileIdentifier = fileStore.uploadStream(sha512ingInputStream, originalFilename);

            return persistDeduplicateAndUpdateFileMapping(sha256ingInputStream.hash().toString(),
                    sha512ingInputStream.hash().toString(), fileIdentifier, countingInputStream.getCount());
        }
    }

    /**
     * Stream a file to the file store, recording its name.
     * <p>
     * Callers are responsible for closing the input stream.
     *
     * @param originalFilename to record. Typically the original filename a user would associate with the file contents.
     * @param fileSize         in bytes
     * @param inputStream      containing content to upload. Managed by the caller.
     * @return persisted file information
     */
    public PersistedFile uploadAsStream(String originalFilename, long fileSize, InputStream inputStream) throws IOException {
        try (var sha256ingInputStream = new HashingInputStream(Hashing.sha256(), inputStream);
             var sha512ingInputStream = new HashingInputStream(Hashing.sha512(), sha256ingInputStream)) {
            var fileIdentifier = fileStore.uploadStream(sha512ingInputStream, originalFilename, fileSize);

            return persistDeduplicateAndUpdateFileMapping(sha256ingInputStream.hash().toString(),
                    sha512ingInputStream.hash().toString(), fileIdentifier, fileSize);
        }
    }

    /**
     * Streaming download.
     * <p>
     * Callers are responsible for closing the returned input stream.
     *
     * @param persistableFileMapping returned when a file was uploaded to the file store
     * @return an input stream of known length
     * @throws IOException if the file doesn't exist or could not be read
     */
    public InputStreamOfKnownLength downloadAsStream(PersistableFileMapping persistableFileMapping) throws IOException {
        PersistedFileIdentifier persistedFileIdentifier = persistableFileMapping.getPersistedFileIdentifier();
        return fileStore.downloadAsStream(persistedFileIdentifier.getNativeStorageFileId());
    }

    private PersistedFile persistDeduplicateAndUpdateFileMapping(String sha256,
                                                                 String sha512,
                                                                 String fileIdentifier, long fileSizeBytes) {
        var persistedFile = deduplicateUploadedFile(fileIdentifier, sha256, sha512, fileSizeBytes, fileStore.nativeStorageType());
        addFileMapping(persistedFile, fileSizeBytes, fileStore.nativeStorageType());
        return persistedFile;
    }

    private PersistedFile deduplicateUploadedFile(String fileIdentifier, String uploadSha256, String uploadSha512,
                                                  long fileSizeBytes, NativeStorageType nativeStorageType) {
        Optional<PersistableFileMapping> existingFileMapping = searchForExistingFileMappingToBothHashes(uploadSha256, uploadSha512);

        if (existingFileMapping.isPresent()) {
            deletePersistedFile(fileIdentifier);
            return new PersistedFile(randomUUID(), fileStoreType, nativeStorageType, existingFileMapping.get().getNativeStorageFileId(),
                    uploadSha256, uploadSha512, fileSizeBytes);
        } else {
            return new PersistedFile(randomUUID(), fileStoreType, nativeStorageType, fileIdentifier, uploadSha256, uploadSha512,
                    fileSizeBytes);
        }
    }

    protected Optional<PersistableFileMapping> searchForExistingFileMappingToBothHashes(String uploadSha256, String uploadSha512) {
        var fileMappingExample = new PersistableFileMapping();
        fileMappingExample.setSha256(uploadSha256);
        fileMappingExample.setSha512(uploadSha512);
        fileMappingExample.setMarkedForDeletion(false);
        var matchingFiles = fileMappingRepository.findAll(Example.of(fileMappingExample));
        return matchingFiles.isEmpty() ? Optional.empty() : Optional.of(matchingFiles.get(0));
    }

    private void addFileMapping(PersistedFile persistedFile, long fileSizeBytes, NativeStorageType nativeStorageType) {
        fileMappingRepository.save(new PersistableFileMapping(persistedFile.getFileId(), fileStoreType, nativeStorageType,
                persistedFile.getNativeStorageFileId(), persistedFile.getSha256(), persistedFile.getSha512(), fileSizeBytes, false));
    }

    private void deletePersistedFile(String fileIdentifier) {
        fileStore.delete(fileIdentifier);
    }
}
