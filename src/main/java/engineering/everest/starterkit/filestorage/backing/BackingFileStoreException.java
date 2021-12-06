package engineering.everest.starterkit.filestorage.backing;

public class BackingFileStoreException extends RuntimeException {

    public BackingFileStoreException(String message) {
        super(message);
    }

    public BackingFileStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
