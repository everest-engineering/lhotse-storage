package engineering.everest.starterkit.filestorage;

import java.io.IOException;
import java.io.InputStream;

public interface DeduplicatingFileStore {

    PersistedFile store(String originalFilename, InputStream inputStream) throws IOException;

    InputStream stream(PersistedFileIdentifier persistedFileIdentifier) throws IOException;
}
