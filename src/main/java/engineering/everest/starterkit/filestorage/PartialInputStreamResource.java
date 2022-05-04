package engineering.everest.starterkit.filestorage;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.support.ResourceRegion;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

/**
 * Partial input stream resource, glue code between a ResourceRegion and PartialInputStream.
 *
 * @see ResourceRegion
 * @see PartialInputStream
 * @see FileResourceRegionFactory
 *
 */
@EqualsAndHashCode(callSuper = false)
@ToString
public class PartialInputStreamResource extends AbstractResource {

    private final FileService fileService;
    private final long actualFileSize;
    private final UUID fileId;
    private final long startOffset;
    private final long endOffset;

    public PartialInputStreamResource(long actualFileSize, FileService fileService, UUID fileId, long startOffset, long endOffset) {
        super();
        this.fileService = fileService;
        this.fileId = fileId;
        this.actualFileSize = actualFileSize;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    @Override
    public String getDescription() {
        return String.format("Partial input stream resource for file %s", fileId);
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return new PartialInputStream(fileService.stream(fileId, startOffset, endOffset).getInputStream(), startOffset);
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public boolean isFile() {
        return false;
    }

    @Override
    public boolean isReadable() {
        return true;
    }

    @Override
    public long contentLength() {
        return actualFileSize;
    }
}
