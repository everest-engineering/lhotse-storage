package engineering.everest.starterkit.filestorage;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.InputStream;

@Data
@AllArgsConstructor
public class InputStreamOfKnownLength {
    private InputStream inputStream;
    private long length;
}
