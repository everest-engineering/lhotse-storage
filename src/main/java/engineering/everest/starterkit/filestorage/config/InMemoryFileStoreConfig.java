package engineering.everest.starterkit.filestorage.config;

import engineering.everest.starterkit.filestorage.FileStore;
import engineering.everest.starterkit.filestorage.filestores.InMemoryFileStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "application.filestore.backend", havingValue = "inMemory")
public class InMemoryFileStoreConfig {

    @Bean
    @Qualifier("permanentFileStore")
    FileStore mongoGridFsPermanentFileStoreTemplate() {
        return new InMemoryFileStore();
    }

    @Bean
    @Qualifier("ephemeralFileStore")
    FileStore mongoGridFsEphemeralFileStoreTemplate() {
        return new InMemoryFileStore();
    }
}
