package engineering.everest.starterkit.filestorage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PartialInputStreamResourceTest {

    private static final UUID FILE_ID = randomUUID();
    private static final long CONTENT_LENGTH = 42L;
    private static final long START_OFFSET = 30L;
    private static final long END_OFFSET = 90L;

    private PartialInputStreamResource partialInputStreamResource;

    @Mock
    private FileService fileService;

    @BeforeEach
    void setUp() {
        partialInputStreamResource = new PartialInputStreamResource(CONTENT_LENGTH, fileService, FILE_ID, START_OFFSET, END_OFFSET);
    }

    @Test
    void getDescription_WillBeSomewhatDescriptive() {
        assertEquals("Partial input stream resource for file " + FILE_ID, partialInputStreamResource.getDescription());
    }

    @Test
    void contentLength_WillReturnValuePassedAtCreationTime() {
        assertEquals(CONTENT_LENGTH, partialInputStreamResource.contentLength());
    }

    @Test
    void exists_WillReturnTrue() {
        assertTrue(partialInputStreamResource.exists());
    }

    @Test
    void isReadable_WillReturnTrue() {
        assertTrue(partialInputStreamResource.isReadable());
    }

    @Test
    void isFile_WillReturnFalse() {
        assertFalse(partialInputStreamResource.isFile());
    }

    @Test
    void isOpen_WillReturnFalse() {
        assertFalse(partialInputStreamResource.isOpen());
    }

    @Test
    void getInputStream_WillRetrievePartialInputStream() throws IOException {
        var inputStream = mock(InputStream.class);
        var streamOfKnownLength = mock(InputStreamOfKnownLength.class);
        when(streamOfKnownLength.getInputStream()).thenReturn(inputStream);

        when(fileService.stream(FILE_ID, START_OFFSET, END_OFFSET)).thenReturn(streamOfKnownLength);

        assertEquals(new PartialInputStream(inputStream, START_OFFSET), partialInputStreamResource.getInputStream());
    }
}
