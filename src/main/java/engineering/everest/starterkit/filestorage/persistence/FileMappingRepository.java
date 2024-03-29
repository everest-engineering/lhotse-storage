package engineering.everest.starterkit.filestorage.persistence;

import engineering.everest.starterkit.filestorage.filestores.FileStoreType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface FileMappingRepository extends JpaRepository<PersistableFileMapping, UUID> {

    List<PersistableFileMapping> findByMarkedForDeletionTrue(Pageable pageable);

    List<PersistableFileMapping> findByFileStoreType(FileStoreType fileStoreType);

    List<PersistableFileMapping> findByBackingStorageFileId(String backingStorageFileId);
}
