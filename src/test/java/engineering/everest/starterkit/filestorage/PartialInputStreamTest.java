package engineering.everest.starterkit.filestorage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static java.util.Arrays.copyOfRange;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class PartialInputStreamTest {

    private final static long SKIP_BYTES = 20;
    private final static byte[] PARTIAL_FILE_CONTENTS = "this could be anything but most likely to be a streaming video".getBytes();

    private PartialInputStream partialInputStream;

    @BeforeEach
    void setUp() {
        partialInputStream = new PartialInputStream(new ByteArrayInputStream(PARTIAL_FILE_CONTENTS), 20);
    }

    @Test
    void skip_WillReturnNumberOfBytesSkipped() throws IOException {
        assertEquals(SKIP_BYTES, partialInputStream.skip(SKIP_BYTES));
    }

    @Test
    void skip_WillTrackNumberOfBytesBeforePartialContentReached() throws IOException {
        var backingInputStream = mock(InputStream.class);
        partialInputStream = new PartialInputStream(backingInputStream, SKIP_BYTES);

        partialInputStream.skip(5);
        assertEquals(SKIP_BYTES - 5, partialInputStream.skip(SKIP_BYTES));
        verifyNoMoreInteractions(backingInputStream);
    }

    @Test
    void skip_WillCallBackingStream_WhenPartialFileContentsAlreadySkipped() throws IOException {
        var backingInputStream = mock(InputStream.class);
        partialInputStream = new PartialInputStream(backingInputStream, SKIP_BYTES);

        partialInputStream.skip(SKIP_BYTES);
        partialInputStream.skip(SKIP_BYTES);
        verify(backingInputStream).skip(SKIP_BYTES);
        verifyNoMoreInteractions(backingInputStream);
    }

    @Test
    void readSingleByte_WillSucceed_WhenStartOfStreamSkipped() throws IOException {
        partialInputStream.skip(SKIP_BYTES);
        assertEquals(PARTIAL_FILE_CONTENTS[0], partialInputStream.read());
    }

    @Test
    void readSingleByte_WillFail_WhenStartOfFileNotSkipped() {
        var exception = assertThrows(IOException.class, () -> partialInputStream.read());
        assertEquals("You must skip over the partial (missing) portion of this input stream", exception.getMessage());
    }

    @Test
    void readIntoArray_WillSucceed_WhenStartOfStreamSkipped() throws IOException {
        byte[] target = new byte[PARTIAL_FILE_CONTENTS.length];

        partialInputStream.skip(SKIP_BYTES);
        partialInputStream.read(target);
        assertArrayEquals(PARTIAL_FILE_CONTENTS, target);
    }

    @Test
    void readIntoArray_WillFail_WhenStartOfFileNotSkipped() {
        var exception = assertThrows(IOException.class, () -> partialInputStream.read(new byte[999]));
        assertEquals("You must skip over the partial (missing) portion of this input stream", exception.getMessage());
    }

    @Test
    void readAllBytes_WillSucceed_WhenStartOfStreamSkipped() throws IOException {
        partialInputStream.skip(SKIP_BYTES);
        assertArrayEquals(PARTIAL_FILE_CONTENTS, partialInputStream.readAllBytes());
    }

    @Test
    void readAllBytes_WillFail_WhenStartOfFileNotSkipped() {
        var exception = assertThrows(IOException.class, () -> partialInputStream.readAllBytes());
        assertEquals("You must skip over the partial (missing) portion of this input stream", exception.getMessage());
    }

    @Test
    void readNBytes_WillSucceed_WhenStartOfStreamSkipped() throws IOException {
        partialInputStream.skip(SKIP_BYTES);
        assertArrayEquals(copyOfRange(PARTIAL_FILE_CONTENTS, 0, 10), partialInputStream.readNBytes(10));
    }

    @Test
    void readNBytes_WillFail_WhenStartOfFileNotSkipped() {
        var exception = assertThrows(IOException.class, () -> partialInputStream.readNBytes(10));
        assertEquals("You must skip over the partial (missing) portion of this input stream", exception.getMessage());
    }

    @Test
    void transferTo_WillSucceed_WhenStartOfStreamSkipped() throws IOException {
        var outputStream = new ByteArrayOutputStream(999);
        partialInputStream.skip(SKIP_BYTES);
        partialInputStream.transferTo(outputStream);
        assertArrayEquals(outputStream.toByteArray(), PARTIAL_FILE_CONTENTS);
    }

    @Test
    void transferTo_WillFail_WhenStartOfFileNotSkipped() {
        var exception = assertThrows(IOException.class, () -> partialInputStream.transferTo(mock(OutputStream.class)));
        assertEquals("You must skip over the partial (missing) portion of this input stream", exception.getMessage());
    }

    @Test
    void reset_WillFail() {
        var exception = assertThrows(IOException.class, () -> partialInputStream.reset());
        assertEquals("mark/reset not supported", exception.getMessage());
    }

    @Test
    void close_WillCloseBackingStream() throws IOException {
        var backingInputStream = mock(InputStream.class);
        partialInputStream = new PartialInputStream(backingInputStream, SKIP_BYTES);
        partialInputStream.close();
        verify(backingInputStream).close();
    }
}
