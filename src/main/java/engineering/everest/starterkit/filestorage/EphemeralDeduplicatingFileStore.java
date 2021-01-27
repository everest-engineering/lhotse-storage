package engineering.everest.starterkit.filestorage;

import engineering.everest.starterkit.filestorage.persistence.FileMappingRepository;
import engineering.everest.starterkit.filestorage.persistence.PersistableFileMapping;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static engineering.everest.starterkit.filestorage.FileStoreType.EPHEMERAL;

/**
 * File store that removes duplicate copies of files and manages the mapping of individual file uploads
 * to a single backing file.
 * <p>
 * This implementation augments the default deduplicating file store by adding the ability to delete.
 * It can not be used to delete files added by the base class.
 *
 * @see PermanentDeduplicatingFileStore
 */
public class EphemeralDeduplicatingFileStore extends PermanentDeduplicatingFileStore {

    public EphemeralDeduplicatingFileStore(FileMappingRepository fileMappingRepository,
                                           FileStore fileStore) {
        super(EPHEMERAL, fileMappingRepository, fileStore);
    }

    /**
     * Delete a set of files
     *
     * @param persistedFileIdentifiers of the files to be deleted
     * @throws IllegalArgumentException when a file is not ephemeral
     */
    public void deleteFiles(Set<PersistedFileIdentifier> persistedFileIdentifiers) {
        persistedFileIdentifiers.forEach(this::deleteFile);
    }

    /**
     * Delete a single file
     *
     * @param persistedFileIdentifier of the file to be deleted
     * @throws IllegalArgumentException when the file is not ephemeral
     */
    public void deleteFile(PersistedFileIdentifier persistedFileIdentifier) {
        checkArgument(persistedFileIdentifier.getFileStoreType() == EPHEMERAL);

        fileMappingRepository.findById(persistedFileIdentifier.getFileId())
                .ifPresent(this::markPersistedFileForDeletion);
    }

    private void markPersistedFileForDeletion(PersistableFileMapping persistableFileMapping) {
        persistableFileMapping.setMarkedForDeletion(true);
        fileMappingRepository.save(persistableFileMapping);
    }
}
