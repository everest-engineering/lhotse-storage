package engineering.everest.starterkit.filestorage;

import engineering.everest.starterkit.filestorage.persistence.FileMappingRepository;
import engineering.everest.starterkit.filestorage.persistence.PersistableFileMapping;
import org.springframework.data.domain.Pageable;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static engineering.everest.starterkit.filestorage.FileStoreType.EPHEMERAL;
import static java.util.stream.Collectors.toSet;
import static org.springframework.data.domain.PageRequest.of;

/**
 * File store that removes duplicate copies of files and manages the mapping of individual file uploads to a single backing file.
 * <p>
 * This implementation augments the default deduplicating file store by adding the ability to delete. It can not be used to delete files
 * added by the base class.
 *
 * @see PermanentDeduplicatingFileStore
 */
public class EphemeralDeduplicatingFileStore extends PermanentDeduplicatingFileStore {

    public EphemeralDeduplicatingFileStore(FileMappingRepository fileMappingRepository,
                                           FileStore fileStore) {
        super(EPHEMERAL, fileMappingRepository, fileStore);
    }

    @Override
    public InputStreamOfKnownLength downloadAsStream(PersistableFileMapping persistableFileMapping) throws IOException {
        if (persistableFileMapping.isMarkedForDeletion()) {
            throw new NoSuchElementException("Ephemeral file not found");
        }
        return super.downloadAsStream(persistableFileMapping);
    }

    public void markFilesForDeletion(Set<PersistedFileIdentifier> persistedFileIdentifiers) {
        persistedFileIdentifiers.forEach(this::markFileForDeletion);
    }

    public void markFileForDeletion(PersistedFileIdentifier persistedFileIdentifier) {
        checkArgument(persistedFileIdentifier.getFileStoreType() == EPHEMERAL);

        fileMappingRepository.findById(persistedFileIdentifier.getFileId())
            .ifPresent(this::markPersistedFileForDeletion);
    }

    public void markAllFilesForDeletion() {
        List<PersistableFileMapping> persistableFileMappings = fileMappingRepository.findAll();
        persistableFileMappings.forEach(persistableFileMapping -> persistableFileMapping.setMarkedForDeletion(true));
        fileMappingRepository.saveAll(persistableFileMappings);
    }

    public void deleteFileBatch(int batchSize) {
        Pageable pageable = of(0, batchSize);
        Set<String> filesInBatch = fileMappingRepository.findByMarkedForDeletionTrue(pageable).stream()
            .map(PersistableFileMapping::getNativeStorageFileId)
            .collect(toSet());

        fileStore.deleteFiles(filesInBatch);
        fileMappingRepository.deleteAllByNativeStorageFileIdIn(filesInBatch);
    }

    private void markPersistedFileForDeletion(PersistableFileMapping persistableFileMapping) {
        persistableFileMapping.setMarkedForDeletion(true);
        fileMappingRepository.save(persistableFileMapping);
    }
}
