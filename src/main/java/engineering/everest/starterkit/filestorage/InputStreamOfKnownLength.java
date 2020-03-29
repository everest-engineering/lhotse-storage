package engineering.everest.starterkit.filestorage;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.InputStream;

@Data
@AllArgsConstructor
public class InputStreamOfKnownLength implements AutoCloseable {
    private InputStream inputStream;
    private long length;

    @Override
    public void close() throws Exception {
        inputStream.close();
    }
}
