package engineering.everest.starterkit.filestorage.filestores;

import engineering.everest.starterkit.filestorage.FileStore;
import engineering.everest.starterkit.filestorage.InputStreamOfKnownLength;
import engineering.everest.starterkit.filestorage.NativeStorageType;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static engineering.everest.starterkit.filestorage.NativeStorageType.IN_MEMORY;
import static java.util.UUID.randomUUID;

/**
 * An filestore suitable for development. Not recommended for use in production.
 */
public class InMemoryFileStore implements FileStore {
    private final Map<String, Metadata> fileMapping;

    public InMemoryFileStore() {
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
            throw new RuntimeException("Unable to upload file " + fileName);
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
            throw new RuntimeException("Unable to upload file " + fileName);
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
    public NativeStorageType nativeStorageType() {
        return IN_MEMORY;
    }

    private void throwIfFileNotInFilestore(String fileIdentifier) {
        if (!fileMapping.containsKey(fileIdentifier)) {
            throw new RuntimeException(String.format("File '%s' not in filestore", fileIdentifier));
        }
    }

    private void throwIfContentLengthNotExpectedFileSize(String fileName, long fileSize, byte[] contents) {
        if (fileSize != contents.length) {
            throw new RuntimeException(
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
