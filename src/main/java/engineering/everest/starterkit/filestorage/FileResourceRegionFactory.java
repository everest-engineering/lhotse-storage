package engineering.everest.starterkit.filestorage;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.support.ResourceRegion;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

import java.util.UUID;

import static java.lang.Math.min;

/**
 * Convenience factory for creating file resource regions for use in streaming partial file content.
 *
 * Example:
 *
 * <pre>
 * {@code
 * public ResponseEntity<ResourceRegion> streamVideo(@RequestHeader HttpHeaders headers) {
 *     var fileLength = fileService.fileSizeInBytes(SOME_FILE_ID);
 *     var resourceRegion = fileResourceRegionFactory.createFileResourceRegion(
 *         SOME_FILE_ID, headers, fileLength);
 *
 *     return ResponseEntity.status(PARTIAL_CONTENT)
 *         .contentType("video/mp4")
 *         .body(resourceRegion);
 * }}
 * </pre>
 */
@Component
public class FileResourceRegionFactory {
    private final FileService fileService;
    private final long maxChunkSizeInBytes;

    public FileResourceRegionFactory(FileService fileService,
                                     @Value("${application.filestore.streaming.chunk.max.bytes:10485760}") String maxChunkSizeInBytes) {
        this.fileService = fileService;
        this.maxChunkSizeInBytes = Long.parseLong(maxChunkSizeInBytes);
    }

    /**
     * Creates a resource region for a given file accessible through the file service. The maximum length of the resource region is capped
     * at {@link FileResourceRegionFactory#maxChunkSizeInBytes}
     *
     * @param  fileId  of a previously stored file
     * @param  headers HTTP request headers
     * @return         a resource region describing the partial content being returned
     */
    public ResourceRegion createFileResourceRegion(UUID fileId, HttpHeaders headers) {
        var actualFileSize = fileService.fileSizeInBytes(fileId);

        var range = headers.getRange().stream().findFirst();
        if (range.isPresent()) {
            var requestedStartIndex = range.get().getRangeStart(actualFileSize);
            var requestedEndIndex = range.get().getRangeEnd(actualFileSize);

            if ((requestedEndIndex - requestedStartIndex) > maxChunkSizeInBytes) {
                var partialInputStreamResource = new PartialInputStreamResource(actualFileSize, fileService, fileId,
                    requestedStartIndex, requestedStartIndex + maxChunkSizeInBytes - 1);
                return new ResourceRegion(partialInputStreamResource, requestedStartIndex, maxChunkSizeInBytes);
            } else {
                var partialInputStreamResource = new PartialInputStreamResource(actualFileSize, fileService, fileId,
                    requestedStartIndex, requestedEndIndex);
                return new ResourceRegion(partialInputStreamResource, requestedStartIndex, requestedEndIndex - requestedStartIndex + 1);
            }
        } else {
            var count = min(maxChunkSizeInBytes, actualFileSize);
            return new ResourceRegion(new PartialInputStreamResource(actualFileSize, fileService, fileId, 0, count - 1), 0, count);
        }
    }
}
