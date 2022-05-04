package engineering.everest.starterkit.filestorage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRange;

import java.util.List;
import java.util.UUID;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.springframework.http.HttpHeaders.EMPTY;

@ExtendWith(MockitoExtension.class)
class FileResourceRegionFactoryTest {

    private static final UUID FILE_ID = randomUUID();
    private static final long FILE_LENGTH = 8888L;
    private static final long MAX_CHUNK_SIZE = 10_485_760L;
    private static final long HUGE_FILE_LENGTH = 999_999_999_999L;

    private FileResourceRegionFactory fileResourceRegionFactory;

    @Mock
    private FileService fileService;

    @BeforeEach
    void setUp() {
        when(fileService.fileSizeInBytes(FILE_ID)).thenReturn(FILE_LENGTH);
        fileResourceRegionFactory = new FileResourceRegionFactory(fileService, Long.toString(MAX_CHUNK_SIZE));
    }

    @Test
    void willReturnFullStreamWhenNoRangeIsRequested() {
        var expectedPartialInputStreamResource =
            new PartialInputStreamResource(FILE_LENGTH, fileService, FILE_ID, 0L, FILE_LENGTH - 1);
        var twineResourceRegion = fileResourceRegionFactory.createFileResourceRegion(FILE_ID, EMPTY);

        assertEquals(expectedPartialInputStreamResource, twineResourceRegion.getResource());
        assertEquals(0L, twineResourceRegion.getPosition());
        assertEquals(FILE_LENGTH, twineResourceRegion.getCount());
    }

    @Test
    void willReturnFullStreamLimitedToMaximumChunkSizeWhenNoRangeIsRequested() {
        when(fileService.fileSizeInBytes(FILE_ID)).thenReturn(HUGE_FILE_LENGTH);

        var expectedPartialInputStreamResource =
            new PartialInputStreamResource(HUGE_FILE_LENGTH, fileService, FILE_ID, 0L, MAX_CHUNK_SIZE - 1);
        var twineResourceRegion = fileResourceRegionFactory.createFileResourceRegion(FILE_ID, EMPTY);

        assertEquals(expectedPartialInputStreamResource, twineResourceRegion.getResource());
        assertEquals(0L, twineResourceRegion.getPosition());
        assertEquals(MAX_CHUNK_SIZE, twineResourceRegion.getCount());
    }

    @Test
    void willReturnPartialStreamFromStartIndexToEndOfFile_WhenOnlyStartRequested() {
        var expectedPartialInputStreamResource =
            new PartialInputStreamResource(FILE_LENGTH, fileService, FILE_ID, 42L, FILE_LENGTH - 1);
        var httpHeaders = new HttpHeaders();
        httpHeaders.setRange(List.of(HttpRange.createByteRange(42L)));
        var twineResourceRegion = fileResourceRegionFactory.createFileResourceRegion(FILE_ID, httpHeaders);

        assertEquals(expectedPartialInputStreamResource, twineResourceRegion.getResource());
        assertEquals(42L, twineResourceRegion.getPosition());
        assertEquals(FILE_LENGTH - 42L, twineResourceRegion.getCount());
    }

    @Test
    void willReturnPartialStreamLimitedToMaximumChunkSize_WhenOnlyStartRequested() {
        when(fileService.fileSizeInBytes(FILE_ID)).thenReturn(HUGE_FILE_LENGTH);

        var expectedPartialInputStreamResource =
            new PartialInputStreamResource(HUGE_FILE_LENGTH, fileService, FILE_ID, 42L, 42L + MAX_CHUNK_SIZE - 1);
        var httpHeaders = new HttpHeaders();
        httpHeaders.setRange(List.of(HttpRange.createByteRange(42L)));
        var twineResourceRegion = fileResourceRegionFactory.createFileResourceRegion(FILE_ID, httpHeaders);

        assertEquals(expectedPartialInputStreamResource, twineResourceRegion.getResource());
        assertEquals(42L, twineResourceRegion.getPosition());
        assertEquals(MAX_CHUNK_SIZE, twineResourceRegion.getCount());
    }

    @Test
    void willReturnPartialStreamFromStartIndexToEndIndex_WhenRangeRequested() {
        var expectedPartialInputStreamResource = new PartialInputStreamResource(FILE_LENGTH, fileService, FILE_ID, 42L, 8000L - 1L);
        var httpHeaders = new HttpHeaders();
        httpHeaders.setRange(List.of(HttpRange.createByteRange(42L, 8000L - 1L)));
        var twineResourceRegion = fileResourceRegionFactory.createFileResourceRegion(FILE_ID, httpHeaders);

        assertEquals(expectedPartialInputStreamResource, twineResourceRegion.getResource());
        assertEquals(42L, twineResourceRegion.getPosition());
        assertEquals(7958L, twineResourceRegion.getCount());
    }

    @Test
    void willReturnPartialStreamLimitedToMaximumChunkSizeFromStartIndexToEndIndex_WhenRangeRequested() {
        when(fileService.fileSizeInBytes(FILE_ID)).thenReturn(HUGE_FILE_LENGTH);

        var expectedPartialInputStreamResource = new PartialInputStreamResource(HUGE_FILE_LENGTH, fileService, FILE_ID,
            42L, 42L + MAX_CHUNK_SIZE - 1L);
        var httpHeaders = new HttpHeaders();
        httpHeaders.setRange(List.of(HttpRange.createByteRange(42L, MAX_CHUNK_SIZE * 2)));
        var twineResourceRegion = fileResourceRegionFactory.createFileResourceRegion(FILE_ID, httpHeaders);

        assertEquals(expectedPartialInputStreamResource, twineResourceRegion.getResource());
        assertEquals(42L, twineResourceRegion.getPosition());
        assertEquals(MAX_CHUNK_SIZE, twineResourceRegion.getCount());
    }
}
