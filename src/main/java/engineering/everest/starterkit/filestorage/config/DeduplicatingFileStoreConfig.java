package engineering.everest.starterkit.filestorage.config;

import engineering.everest.starterkit.filestorage.DeduplicatingFileStore;
import engineering.everest.starterkit.filestorage.FileService;
import engineering.everest.starterkit.filestorage.FileStore;
import engineering.everest.starterkit.filestorage.DefaultDeduplicatingFileStore;
import engineering.everest.starterkit.filestorage.persistence.FileMappingRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static engineering.everest.starterkit.filestorage.FileStoreType.EPHEMERAL;
import static engineering.everest.starterkit.filestorage.FileStoreType.PERMANENT;

@Configuration
public class DeduplicatingFileStoreConfig {

    @Bean
    @Qualifier("permanentDeduplicatingFileStore")
    DeduplicatingFileStore permanentFileStore(FileMappingRepository fileMappingRepository,
                                              @Qualifier("permanentFileStore") FileStore fileStore) {
        return new DefaultDeduplicatingFileStore(PERMANENT, fileMappingRepository, fileStore);
    }

    @Bean
    @Qualifier("ephemeralDeduplicatingFileStore")
    DeduplicatingFileStore ephemeralFileStore(FileMappingRepository fileMappingRepository,
                                              @Qualifier("ephemeralFileStore") FileStore fileStore) {
        return new DefaultDeduplicatingFileStore(EPHEMERAL, fileMappingRepository, fileStore);
    }

    @Bean
    public FileService fileService(FileMappingRepository fileMappingRepository,
                                   @Qualifier("permanentDeduplicatingFileStore") DeduplicatingFileStore permanentDeduplicatingFileStore,
                                   @Qualifier("ephemeralDeduplicatingFileStore") DeduplicatingFileStore ephemeralDeduplicatingFileStore) {
        return new FileService(fileMappingRepository, permanentDeduplicatingFileStore, ephemeralDeduplicatingFileStore);
    }
}
