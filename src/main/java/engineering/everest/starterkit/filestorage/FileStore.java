package engineering.everest.starterkit.filestorage;

import java.io.IOException;
import java.io.InputStream;

public interface FileStore {

    String create(InputStream inputStream, String fileName);

    void delete(String fileIdentifier);

    InputStream read(String fileIdentifier) throws IOException;

    NativeStorageType nativeStorageType();
}