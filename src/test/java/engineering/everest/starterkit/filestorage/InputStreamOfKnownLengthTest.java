package engineering.everest.starterkit.filestorage;

import org.junit.jupiter.api.Test;

import java.lang.reflect.AnnotatedType;
import java.util.List;

import static java.util.Arrays.asList;

class InputStreamOfKnownLengthTest {

    @Test
    void isAutoCloseable() {
        List<AnnotatedType> annotatedTypes = asList(InputStreamOfKnownLength.class.getAnnotatedInterfaces());
        annotatedTypes.stream().filter(x -> x.getType().equals(AutoCloseable.class))
                .findFirst()
                .orElseThrow();
    }
}