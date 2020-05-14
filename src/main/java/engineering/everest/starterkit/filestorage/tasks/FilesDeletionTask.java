package engineering.everest.starterkit.filestorage.tasks;

import engineering.everest.starterkit.filestorage.FileStore;
import engineering.everest.starterkit.filestorage.persistence.FileMappingRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class FilesDeletionTask {

    private final FileMappingRepository fileMappingRepository;
    private final FileStore fileStore;

    @Autowired
    public FilesDeletionTask(FileMappingRepository fileMappingRepository, FileStore fileStore) {
        this.fileMappingRepository = fileMappingRepository;
        this.fileStore = fileStore;
    }

    @Scheduled(fixedRateString = "PT${storage.files.deletion.fixedRate:5m}")
    void checkForFilesToDelete() {
        fileMappingRepository.findTop500ByMarkedForDeletionTrue()
                .stream()
                .forEach(persistableFileMapping -> fileStore.delete(persistableFileMapping.getNativeStorageFileId()));

    }
}
