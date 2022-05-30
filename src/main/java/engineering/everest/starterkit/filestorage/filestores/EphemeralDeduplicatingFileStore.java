package engineering.everest.starterkit.filestorage.filestores;

import engineering.everest.starterkit.filestorage.backing.BackingStore;
import engineering.everest.starterkit.filestorage.InputStreamOfKnownLength;
import engineering.everest.starterkit.filestorage.PersistedFileIdentifier;
import engineering.everest.starterkit.filestorage.persistence.FileMappingRepository;
import engineering.everest.starterkit.filestorage.persistence.PersistableFileMapping;
import org.springframework.data.domain.PageRequest;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkArgument;
import static engineering.everest.starterkit.filestorage.filestores.FileStoreType.EPHEMERAL;
import static java.util.stream.Collectors.toSet;

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
                                           BackingStore backingStore) {
        super(EPHEMERAL, fileMappingRepository, backingStore);
    }

    @Override
    public InputStreamOfKnownLength downloadAsStream(PersistableFileMapping persistableFileMapping) throws IOException {
        if (persistableFileMapping.isMarkedForDeletion()) {
            throw new NoSuchElementException("Ephemeral file not found");
        }
        return super.downloadAsStream(persistableFileMapping);
    }

    public void markFilesForDeletion(List<PersistedFileIdentifier> persistedFileIdentifiers) {
        persistedFileIdentifiers.forEach(identifier -> checkArgument(identifier.getFileStoreType() == EPHEMERAL));
        persistedFileIdentifiers.forEach(identifier -> fileMappingRepository.findById(identifier.getFileId())
            .ifPresent(this::markPersistedFileForDeletion));
    }

    public void markFileForDeletion(PersistedFileIdentifier persistedFileIdentifier) {
        checkArgument(persistedFileIdentifier.getFileStoreType() == EPHEMERAL);

        fileMappingRepository.findById(persistedFileIdentifier.getFileId())
            .ifPresent(this::markPersistedFileForDeletion);
    }

    public void markAllFilesForDeletion() {
        var persistableFileMappings = fileMappingRepository.findByFileStoreType(EPHEMERAL);
        persistableFileMappings.forEach(persistableFileMapping -> persistableFileMapping.setMarkedForDeletion(true));
        fileMappingRepository.saveAll(persistableFileMappings);
    }

    public void deleteFileBatch(int batchSize) {
        var pageable = PageRequest.of(0, batchSize);
        var filesInBatch = fileMappingRepository.findByMarkedForDeletionTrue(pageable).stream()
            .map(PersistableFileMapping::getBackingStorageFileId)
            .collect(toSet());

        backingStore.deleteFiles(filesInBatch);
        fileMappingRepository.deleteAllByBackingStorageFileIdIn(filesInBatch);
    }

    private void markPersistedFileForDeletion(PersistableFileMapping persistableFileMapping) {
        persistableFileMapping.setMarkedForDeletion(true);
        fileMappingRepository.save(persistableFileMapping);
    }
}
