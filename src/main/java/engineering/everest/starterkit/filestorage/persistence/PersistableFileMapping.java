package engineering.everest.starterkit.filestorage.persistence;

import engineering.everest.starterkit.filestorage.FileStoreType;
import engineering.everest.starterkit.filestorage.NativeStorageType;
import engineering.everest.starterkit.filestorage.PersistedFileIdentifier;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "fs.mappings")
public class PersistableFileMapping {

    @Id
    private UUID fileId;
    private FileStoreType fileStoreType;
    private NativeStorageType nativeStorageType;
    private String nativeStorageFileId;
    private String sha256;
    private String sha512;
    private Long fileSizeBytes;

    public PersistedFileIdentifier getPersistedFileIdentifier() {
        return new PersistedFileIdentifier(fileId, fileStoreType, nativeStorageType, nativeStorageFileId);
    }
}