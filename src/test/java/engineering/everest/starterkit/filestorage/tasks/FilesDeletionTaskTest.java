package engineering.everest.starterkit.filestorage.tasks;

import engineering.everest.starterkit.filestorage.FileStore;
import engineering.everest.starterkit.filestorage.persistence.FileMappingRepository;
import engineering.everest.starterkit.filestorage.persistence.PersistableFileMapping;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.scheduling.annotation.Scheduled;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.UUID;
import java.util.stream.Stream;

import static engineering.everest.starterkit.filestorage.FileStoreType.PERMANENT;
import static engineering.everest.starterkit.filestorage.NativeStorageType.MONGO_GRID_FS;
import static java.time.Duration.parse;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class FilesDeletionTaskTest {

    private static final UUID FILE_ID = UUID.randomUUID();
    private static final String FILE_IDENTIFIER = "FILE_IDENTIFIER";
    private static final String SHA_256 = "SHA_256";
    private static final String SHA_512 = "SHA_512";
    private static final String TEMPORARY_FILE_CONTENTS = "TEMPORARY_FILE_CONTENTS";
    private static final Long FILE_SIZE = (long) TEMPORARY_FILE_CONTENTS.length();

    private static final PersistableFileMapping PERSISTABLE_FILE_MAPPING = new PersistableFileMapping(FILE_ID, PERMANENT, MONGO_GRID_FS, FILE_IDENTIFIER, SHA_256, SHA_512, FILE_SIZE, true);

    @Mock
    private FileStore fileStore;

    @Mock
    private FileMappingRepository fileMappingRepository;

    private FilesDeletionTask filesDeletionTask;

    @BeforeEach
    void setUp() {
        filesDeletionTask = new FilesDeletionTask(fileMappingRepository, fileStore);
    }

    @Test
    void checkForFilesToDelete_IsAnnotatedToRunPeriodically() {
        Method checkForFilesToDeleteMethod = stream(FilesDeletionTask.class.getDeclaredMethods())
                .filter(method -> method.getName().equals("checkForFilesToDelete"))
                .findFirst().orElseThrow();

        Scheduled schedule = checkForFilesToDeleteMethod.getAnnotation(Scheduled.class);
        assertTrue(schedule.fixedRate() > 0 || !schedule.fixedRateString().equals(""));
    }

    @ParameterizedTest
    @MethodSource("exampleTimeStrings")
    void expiryDetectionPeriodCheckRate_WillHandleArbitraryTimeUnits(String input, Duration expectedDuration) {
        Method checkForTimedOutHelpSessionsMethod = stream(FilesDeletionTask.class.getDeclaredMethods())
                .filter(method -> method.getName().equals("checkForFilesToDelete"))
                .findFirst().orElseThrow();
        Scheduled schedule = checkForTimedOutHelpSessionsMethod.getAnnotation(Scheduled.class);

        String expression = schedule.fixedRateString().replace("${storage.files.deletion.fixedRate:5m}", input);
        assertEquals(expectedDuration, parse(expression));
    }

    @Test
    void checkForFilesToDelete_WillHaveNoInteractionsIfThereAreNoFilesToDelete() {
        when(fileMappingRepository.findTop500ByMarkedForDeletionTrue()).thenReturn(emptyList());

        filesDeletionTask.checkForFilesToDelete();

        verifyNoInteractions(fileStore);
    }

    @Test
    void checkForFilesToDelete_WillDeleteFilesFromFileStoreIfThereAreFilesMarkedForDeletion() {
        when(fileMappingRepository.findTop500ByMarkedForDeletionTrue()).thenReturn(asList(PERSISTABLE_FILE_MAPPING));

        filesDeletionTask.checkForFilesToDelete();

        verify(fileStore).delete(FILE_IDENTIFIER);
    }

    private static Stream<Arguments> exampleTimeStrings() {
        return Stream.of(
                Arguments.of("1s", Duration.ofSeconds(1)),
                Arguments.of("2m", Duration.ofMinutes(2)),
                Arguments.of("3h", Duration.ofHours(3)));
    }
}
