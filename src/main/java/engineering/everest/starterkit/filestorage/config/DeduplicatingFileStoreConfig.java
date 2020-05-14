package engineering.everest.starterkit.filestorage.config;

import engineering.everest.starterkit.filestorage.DefaultDeduplicatingFileStore;
import engineering.everest.starterkit.filestorage.EphemeralDeduplicatingFileStore;
import engineering.everest.starterkit.filestorage.FileService;
import engineering.everest.starterkit.filestorage.FileStore;
import engineering.everest.starterkit.filestorage.persistence.FileMappingRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DeduplicatingFileStoreConfig {

    @Bean
    DefaultDeduplicatingFileStore permanentFileStore(FileMappingRepository fileMappingRepository,
                                                     @Qualifier("permanentFileStore") FileStore fileStore) {
        return new DefaultDeduplicatingFileStore(fileMappingRepository, fileStore);
    }

    @Bean
    EphemeralDeduplicatingFileStore ephemeralFileStore(FileMappingRepository fileMappingRepository,
                                                       @Qualifier("ephemeralFileStore") FileStore fileStore) {
        return new EphemeralDeduplicatingFileStore(fileMappingRepository, fileStore);
    }

    @Bean
    public FileService fileService(FileMappingRepository fileMappingRepository,
                                   DefaultDeduplicatingFileStore permanentDeduplicatingFileStore,
                                   EphemeralDeduplicatingFileStore ephemeralDeduplicatingFileStore) {
        return new FileService(fileMappingRepository, permanentDeduplicatingFileStore, ephemeralDeduplicatingFileStore);
    }
}
