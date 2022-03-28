package engineering.everest.starterkit.filestorage;

import engineering.everest.starterkit.filestorage.backing.BackingStorageType;
import engineering.everest.starterkit.filestorage.filestores.FileStoreType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PersistedFileIdentifier implements Serializable {

    private UUID fileId;
    private FileStoreType fileStoreType;
    private BackingStorageType storageType;
    private String backingStorageFileId;
}
