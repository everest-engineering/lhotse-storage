package engineering.everest.starterkit.filestorage;

import java.io.IOException;
import java.io.InputStream;

public interface DeduplicatingFileStore {

    PersistedFile uploadAsStream(String originalFilename, InputStream inputStream) throws IOException;

    PersistedFile uploadAsStream(String originalFilename, long fileSize, InputStream inputStream) throws IOException;

    InputStreamOfKnownLength downloadAsStream(PersistedFileIdentifier persistedFileIdentifier) throws IOException;
}
