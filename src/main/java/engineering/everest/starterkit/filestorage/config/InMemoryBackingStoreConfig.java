package engineering.everest.starterkit.filestorage.config;

import engineering.everest.starterkit.filestorage.BackingStore;
import engineering.everest.starterkit.filestorage.backing.InMemoryBackingStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "application.filestore.backend", havingValue = "inMemory")
public class InMemoryBackingStoreConfig {

    @Bean
    @Qualifier("permanentBackingStore")
    BackingStore inMemoryPermanentBackingStore() {
        return new InMemoryBackingStore();
    }

    @Bean
    @Qualifier("ephemeralBackingStore")
    BackingStore inMemoryEphemeralBackingStore() {
        return new InMemoryBackingStore();
    }
}
