package engineering.everest.starterkit.filestorage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

/**
 * Interface for the low level file store implementations.
 *
 * @see FileService
 * @see engineering.everest.starterkit.filestorage.filestores.AwsS3FileStore
 * @see engineering.everest.starterkit.filestorage.filestores.MongoGridFsFileStore
 */
public interface FileStore {

    /**
     * Stream a file of unknown length to the file store, recording its name.
     * <p>
     * <b>If possible, prefer to call the overloaded method that includes the length of the file. Depending on the backing file store
     * implementation, this method may introduce performance overheads.</b>
     * <p>
     * Callers are responsible for closing the returned input stream.
     *
     * @param  inputStream containing content to upload. Managed by the caller.
     * @param  fileName    to record. Typically the original filename a user would associate with the file contents.
     * @return             a unique string identifying the file. The format is dependent on the backing file store implementation.
     */
    String uploadStream(InputStream inputStream, String fileName);

    /**
     * Stream a file to the file store, recording its name.
     * <p>
     * Callers are responsible for closing the returned input stream.
     *
     * @param  inputStream containing content to upload. Managed by the caller.
     * @param  fileName    to record. Typically the original filename a user would associate with the file contents.
     * @param  fileSize    in bytes
     * @return             a unique string identifying the file. The format is dependent on the backing file store implementation.
     */
    String uploadStream(InputStream inputStream, String fileName, long fileSize);

    /**
     * Delete a file.
     *
     * @param fileIdentifier returned when a file was uploaded to the file store
     */
    void delete(String fileIdentifier);

    /**
     * Streaming download.
     * <p>
     * Callers are responsible for closing the returned input stream.
     *
     * @param  fileIdentifier returned when a file was uploaded to the file store
     * @return                an input stream of known length
     * @throws IOException    if the file doesn't exist or could not be read
     */
    InputStreamOfKnownLength downloadAsStream(String fileIdentifier) throws IOException;

    /**
     * @return the native storage type of the filestore
     */
    NativeStorageType nativeStorageType();

    /**
     * Delete a set of files
     *
     * @param fileIdentifiers of the files to delete
     */
    void deleteFiles(Set<String> fileIdentifiers);
}
