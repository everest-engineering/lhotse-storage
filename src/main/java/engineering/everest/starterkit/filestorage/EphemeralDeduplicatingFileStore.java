package engineering.everest.starterkit.filestorage;

import engineering.everest.starterkit.filestorage.persistence.FileMappingRepository;
import engineering.everest.starterkit.filestorage.persistence.PersistableFileMapping;

import java.util.Set;

import static engineering.everest.starterkit.filestorage.FileStoreType.EPHEMERAL;

public class EphemeralDeduplicatingFileStore extends DefaultDeduplicatingFileStore {

    public EphemeralDeduplicatingFileStore(FileMappingRepository fileMappingRepository,
                                           FileStore fileStore) {
        super(EPHEMERAL, fileMappingRepository, fileStore);
    }

    public void deleteFiles(Set<PersistedFileIdentifier> persistedFileIdentifiers) {
        persistedFileIdentifiers.forEach(this::deleteFile);
    }

    public void deleteFile(PersistedFileIdentifier persistedFileIdentifier) {
        fileMappingRepository.findById(persistedFileIdentifier.getFileId())
                .ifPresent(this::markPersistedFileForDeletion);
    }

    private void markPersistedFileForDeletion(PersistableFileMapping persistableFileMapping) {
        persistableFileMapping.setMarkedForDeletion(true);
        fileMappingRepository.save(persistableFileMapping);
    }
}
