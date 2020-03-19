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

import static java.util.UUID.randomUUID;

public class DefaultDeduplicatingFileStore implements DeduplicatingFileStore {

    private final FileStoreType fileStoreType;
    private final FileMappingRepository fileMappingRepository;
    private FileStore fileStore;

    public DefaultDeduplicatingFileStore(FileStoreType fileStoreType,
                                         FileMappingRepository fileMappingRepository,
                                         FileStore fileStore) {
        this.fileStoreType = fileStoreType;
        this.fileMappingRepository = fileMappingRepository;
        this.fileStore = fileStore;
    }

    @Override
    public PersistedFile uploadAsStream(String originalFilename, InputStream inputStream) throws IOException {
        try (var countingInputStream = new CountingInputStream(inputStream);
             var sha256ingInputStream = new HashingInputStream(Hashing.sha256(), countingInputStream);
             var sha512ingInputStream = new HashingInputStream(Hashing.sha512(), sha256ingInputStream)) {
            var fileIdentifier = fileStore.uploadStream(sha512ingInputStream, originalFilename);

            return persistDeduplicateAndUpdateFileMapping(sha256ingInputStream.hash().toString(),
                    sha512ingInputStream.hash().toString(), fileIdentifier, countingInputStream.getCount());
        }
    }

    @Override
    public PersistedFile uploadAsStream(String originalFilename, long fileSize, InputStream inputStream) throws IOException {
        try (var sha256ingInputStream = new HashingInputStream(Hashing.sha256(), inputStream);
             var sha512ingInputStream = new HashingInputStream(Hashing.sha512(), sha256ingInputStream)) {
            var fileIdentifier = fileStore.uploadStream(sha512ingInputStream, originalFilename, fileSize);

            return persistDeduplicateAndUpdateFileMapping(sha256ingInputStream.hash().toString(),
                    sha512ingInputStream.hash().toString(), fileIdentifier, fileSize);
        }
    }

    @Override
    public InputStreamOfKnownLength downloadAsStream(PersistedFileIdentifier persistedFileIdentifier) throws IOException {
        return fileStore.downloadAsStream(persistedFileIdentifier.getNativeStorageFileId());
    }

    private PersistedFile persistDeduplicateAndUpdateFileMapping(String sha256,
                                                                 String sha512,
                                                                 String fileIdentifier, long fileSizeBytes) {
        var persistedFile = deduplicateUploadedFile(fileIdentifier, sha256, sha512, fileSizeBytes, fileStore.nativeStorageType());
        updateFileMapping(persistedFile, fileSizeBytes, fileStore.nativeStorageType());
        return persistedFile;
    }

    private PersistedFile deduplicateUploadedFile(String fileIdentifier, String uploadSha256, String uploadSha512,
                                                  long fileSizeBytes, NativeStorageType nativeStorageType) {
        Optional<PersistableFileMapping> existingFileMapping = searchForExistingFileMappingToBothHashes(uploadSha256, uploadSha512);

        if (existingFileMapping.isPresent()) {
            fileStore.delete(fileIdentifier);
            return new PersistedFile(randomUUID(), fileStoreType, nativeStorageType, existingFileMapping.get().getNativeStorageFileId(),
                    uploadSha256, uploadSha512, fileSizeBytes);
        } else {
            return new PersistedFile(randomUUID(), fileStoreType, nativeStorageType, fileIdentifier, uploadSha256, uploadSha512,
                    fileSizeBytes);
        }
    }

    private Optional<PersistableFileMapping> searchForExistingFileMappingToBothHashes(String uploadSha256, String uploadSha512) {
        var fileMappingExample = new PersistableFileMapping();
        fileMappingExample.setSha256(uploadSha256);
        fileMappingExample.setSha512(uploadSha512);
        var matchingFiles = fileMappingRepository.findAll(Example.of(fileMappingExample));
        return matchingFiles.isEmpty() ? Optional.empty() : Optional.of(matchingFiles.get(0));
    }

    private void updateFileMapping(PersistedFile persistedFile, long fileSizeBytes, NativeStorageType nativeStorageType) {
        fileMappingRepository.save(new PersistableFileMapping(persistedFile.getFileId(), fileStoreType, nativeStorageType,
                persistedFile.getNativeStorageFileId(), persistedFile.getSha256(), persistedFile.getSha512(), fileSizeBytes));
    }
}
