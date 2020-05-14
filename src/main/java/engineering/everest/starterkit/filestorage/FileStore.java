package engineering.everest.starterkit.filestorage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

public interface FileStore {

    String uploadStream(InputStream inputStream, String fileName);

    String uploadStream(InputStream inputStream, String fileName, long fileSize);

    void delete(String fileIdentifier);

    InputStreamOfKnownLength downloadAsStream(String fileIdentifier) throws IOException;

    NativeStorageType nativeStorageType();

    void deleteFiles(Set<String> fileIdentifiers);
}
