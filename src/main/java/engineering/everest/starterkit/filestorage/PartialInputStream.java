package engineering.everest.starterkit.filestorage;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static java.lang.Math.min;

/**
 * An input stream for wrapping an object store input stream beginning at a non-zero offset. Ensures that the necessary number of bytes have
 * been skipped.
 */
@EqualsAndHashCode(callSuper = false)
@ToString
public class PartialInputStream extends InputStream {

    private final InputStream backingInputStream;
    private final long bytesRequiredToSkip;

    private long bytesSkipped;

    public PartialInputStream(InputStream backingInputStream, long start) {
        super();
        this.backingInputStream = backingInputStream;
        this.bytesRequiredToSkip = start;
    }

    @Override
    public int read() throws IOException {
        throwIfMissingBufferNotSkipped();
        return backingInputStream.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        throwIfMissingBufferNotSkipped();
        return super.read(b);
    }

    @Override
    public byte[] readAllBytes() throws IOException {
        throwIfMissingBufferNotSkipped();
        return super.readAllBytes();
    }

    @Override
    public byte[] readNBytes(int len) throws IOException {
        throwIfMissingBufferNotSkipped();
        return super.readNBytes(len);
    }

    @Override
    public long transferTo(OutputStream out) throws IOException {
        throwIfMissingBufferNotSkipped();
        return super.transferTo(out);
    }

    @Override
    public void close() throws IOException {
        backingInputStream.close();
    }

    /**
     * Skips to the partial content of the underlying input stream by marking progress until the required number of bytes have been skipped.
     * Delegates to the underlying input stream once the partial content has been reached.
     *
     * @param  n           number of bytes to skip
     * @return             number of bytes considered skipped by this call
     * @throws IOException if the skips fails
     */
    @Override
    public long skip(long n) throws IOException {
        if (bytesSkipped < bytesRequiredToSkip) {
            long bytesToSkip = min(bytesRequiredToSkip - bytesSkipped, n);
            bytesSkipped += bytesToSkip;
            return bytesToSkip;
        } else {
            return backingInputStream.skip(n);
        }
    }

    @Override
    @SuppressWarnings("PMD.AvoidSynchronizedAtMethodLevel")
    public synchronized void reset() throws IOException {
        throw new IOException("mark/reset not supported");
    }

    private void throwIfMissingBufferNotSkipped() throws IOException {
        if (bytesSkipped != bytesRequiredToSkip) {
            throw new IOException("You must skip over the partial (missing) portion of this input stream");
        }
    }
}
