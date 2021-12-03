package engineering.everest.starterkit.filestorage.config;

import engineering.everest.starterkit.filestorage.EphemeralDeduplicatingFileStore;
import engineering.everest.starterkit.filestorage.FileService;
import engineering.everest.starterkit.filestorage.BackingStore;
import engineering.everest.starterkit.filestorage.PermanentDeduplicatingFileStore;
import engineering.everest.starterkit.filestorage.persistence.FileMappingRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DeduplicatingFileStoreConfig {

    @Bean
    @Qualifier("permanentDeduplicatingFileStore")
    PermanentDeduplicatingFileStore permanentFileStore(FileMappingRepository fileMappingRepository,
                                                       @Qualifier("permanentBackingStore") BackingStore backingStore) {
        return new PermanentDeduplicatingFileStore(fileMappingRepository, backingStore);
    }

    @Bean
    @Qualifier("ephemeralDeduplicatingFileStore")
    EphemeralDeduplicatingFileStore ephemeralFileStore(FileMappingRepository fileMappingRepository,
                                                       @Qualifier("ephemeralBackingStore") BackingStore backingStore) {
        return new EphemeralDeduplicatingFileStore(fileMappingRepository, backingStore);
    }

    @Bean
    public FileService fileService(
                                   FileMappingRepository fileMappingRepository,
                                   @Qualifier("permanentDeduplicatingFileStore") PermanentDeduplicatingFileStore permanentDeduplicatingFileStore,
                                   @Qualifier("ephemeralDeduplicatingFileStore") EphemeralDeduplicatingFileStore ephemeralDeduplicatingFileStore) {
        return new FileService(fileMappingRepository, permanentDeduplicatingFileStore, ephemeralDeduplicatingFileStore);
    }
}
