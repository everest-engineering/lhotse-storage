package engineering.everest.starterkit.filestorage.backing;

import engineering.everest.starterkit.filestorage.BackingStore;
import engineering.everest.starterkit.filestorage.InputStreamOfKnownLength;
import engineering.everest.starterkit.filestorage.BackingStorageType;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static engineering.everest.starterkit.filestorage.BackingStorageType.IN_MEMORY;
import static java.util.UUID.randomUUID;

/**
 * An filestore suitable for development. Not recommended for use in production.
 */
public class InMemoryBackingStore implements BackingStore {
    private final Map<String, Metadata> fileMapping;

    public InMemoryBackingStore() {
        this.fileMapping = new HashMap<>();
    }

    @Override
    public String uploadStream(InputStream inputStream, String fileName) {
        try {
            var contents = inputStream.readAllBytes();
            var id = randomUUID().toString();
            fileMapping.put(id, new Metadata(contents.length, contents));
            return id;
        } catch (IOException e) {
            throw new BackingFileStoreException("Unable to upload file " + fileName, e);
        }
    }

    @Override
    public String uploadStream(InputStream inputStream, String fileName, long fileSize) {
        try {
            var contents = inputStream.readAllBytes();
            throwIfContentLengthNotExpectedFileSize(fileName, fileSize, contents);

            var id = randomUUID().toString();
            fileMapping.put(id, new Metadata(fileSize, contents));
            return id;
        } catch (IOException e) {
            throw new BackingFileStoreException("Unable to upload file " + fileName, e);
        }
    }

    @Override
    public void delete(String fileIdentifier) {
        throwIfFileNotInFilestore(fileIdentifier);
        fileMapping.remove(fileIdentifier);
    }

    @Override
    public void deleteFiles(Set<String> fileIdentifiers) {
        fileIdentifiers.forEach(this::throwIfFileNotInFilestore);
        fileIdentifiers.forEach(fileMapping::remove);
    }

    @Override
    public InputStreamOfKnownLength downloadAsStream(String fileIdentifier) throws IOException {
        throwIfFileNotInFilestore(fileIdentifier);

        var fileMetadata = fileMapping.get(fileIdentifier);
        return new InputStreamOfKnownLength(new ByteArrayInputStream(fileMetadata.getContent()), fileMetadata.getLength());
    }

    @Override
    public BackingStorageType backingStorageType() {
        return IN_MEMORY;
    }

    private void throwIfFileNotInFilestore(String fileIdentifier) {
        if (!fileMapping.containsKey(fileIdentifier)) {
            throw new BackingFileStoreException(String.format("File '%s' not in filestore", fileIdentifier));
        }
    }

    private void throwIfContentLengthNotExpectedFileSize(String fileName, long fileSize, byte[] contents) {
        if (fileSize != contents.length) {
            throw new BackingFileStoreException(
                String.format("Expected file size %d for uploaded file '%s' but content length is %d", fileSize, fileName,
                    contents.length));
        }
    }

    @AllArgsConstructor
    @Getter
    static class Metadata {
        private final long length;
        private final byte[] content;
    }
}
