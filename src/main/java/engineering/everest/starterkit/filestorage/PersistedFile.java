package engineering.everest.starterkit.filestorage;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PersistedFile implements Serializable {

    private UUID fileId;
    private FileStoreType fileStoreType;
    private BackingStorageType backingStorageType;
    private String backingStorageFileId;
    private String sha256;
    private String sha512;
    private long sizeInBytes;

    PersistedFileIdentifier getPersistedFileIdentifier() {
        return new PersistedFileIdentifier(fileId, fileStoreType, backingStorageType, backingStorageFileId);
    }
}
