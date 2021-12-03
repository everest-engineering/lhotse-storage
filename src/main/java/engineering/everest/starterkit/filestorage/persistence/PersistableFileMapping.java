package engineering.everest.starterkit.filestorage.persistence;

import engineering.everest.starterkit.filestorage.FileStoreType;
import engineering.everest.starterkit.filestorage.BackingStorageType;
import engineering.everest.starterkit.filestorage.PersistedFileIdentifier;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.Id;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity(name = "filemapping")
public class PersistableFileMapping {

    @Id
    private UUID fileId;
    private FileStoreType fileStoreType;
    private BackingStorageType backingStorageType;
    private String backingStorageFileId;
    private String sha256;
    private String sha512;
    private Long fileSizeBytes;
    private boolean markedForDeletion;

    public PersistedFileIdentifier getPersistedFileIdentifier() {
        return new PersistedFileIdentifier(fileId, fileStoreType, backingStorageType, backingStorageFileId);
    }
}
