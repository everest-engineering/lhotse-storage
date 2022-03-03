package engineering.everest.starterkit.filestorage;

import engineering.everest.starterkit.filestorage.backing.AwsS3BackingStore;
import engineering.everest.starterkit.filestorage.backing.MongoGridFsBackingStore;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

/**
 * Interface for the backing file store implementations.
 *
 * @see FileService
 * @see AwsS3BackingStore
 * @see MongoGridFsBackingStore
 */
public interface BackingStore {

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
     * Delete a set of files
     *
     * @param fileIdentifiers of the files to delete
     */
    void deleteFiles(Set<String> fileIdentifiers);

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
     * Streaming download starting from a given offset.
     * <p>
     * Callers are responsible for closing the returned input stream.
     *
     * @param  fileIdentifier returned when a file was uploaded to the file store
     * @param  startingOffset binary offset into the file from which to start streaming from
     * @return                an input stream of known length
     * @throws IOException    if the file doesn't exist or could not be read
     */
    InputStreamOfKnownLength downloadAsStream(String fileIdentifier, long startingOffset) throws IOException;

    /**
     * @return the backing storage type of the filestore
     */
    BackingStorageType backingStorageType();
}
