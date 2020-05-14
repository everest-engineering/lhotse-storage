package engineering.everest.starterkit.filestorage;

import engineering.everest.starterkit.filestorage.persistence.FileMappingRepository;
import engineering.everest.starterkit.filestorage.persistence.PersistableFileMapping;

import java.util.Optional;
import java.util.Set;

import static engineering.everest.starterkit.filestorage.FileStoreType.EPHEMERAL;

public class EphemeralDeduplicatingFileStore extends DefaultDeduplicatingFileStore {

    public EphemeralDeduplicatingFileStore(FileMappingRepository fileMappingRepository,
                                           FileStore fileStore) {
        super(EPHEMERAL, fileMappingRepository, fileStore);
    }

    public void deleteFiles(Set<PersistedFileIdentifier> persistedFileIdentifiers) {
        persistedFileIdentifiers.forEach(persistedFileIdentifier -> deleteFile(persistedFileIdentifier));
    }

    public void deleteFile(PersistedFileIdentifier persistedFileIdentifier) {
        Optional<PersistableFileMapping> optionalPersistableFileMapping = fileMappingRepository.findById(persistedFileIdentifier.getFileId());
        if (optionalPersistableFileMapping.isPresent()) {
            markPersistedFileForDeletion(optionalPersistableFileMapping.get());
        }
    }

    private void markPersistedFileForDeletion(PersistableFileMapping persistableFileMapping) {
        persistableFileMapping.setMarkedForDeletion(true);
        fileMappingRepository.save(persistableFileMapping);
    }
}
