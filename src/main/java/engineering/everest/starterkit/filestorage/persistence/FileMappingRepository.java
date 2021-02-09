package engineering.everest.starterkit.filestorage.persistence;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Set;
import java.util.UUID;

@Repository
public interface FileMappingRepository extends JpaRepository<PersistableFileMapping, UUID> {

    List<PersistableFileMapping> findByMarkedForDeletionTrue(Pageable pageable);

    void deleteAllByNativeStorageFileIdIn(Set<String> nativeStorageFileIds) ;
}
