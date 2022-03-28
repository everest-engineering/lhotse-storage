package engineering.everest.starterkit.filestorage.config;

import engineering.everest.starterkit.filestorage.backing.BackingStore;
import engineering.everest.starterkit.filestorage.backing.MongoGridFsBackingStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.gridfs.GridFsTemplate;

@Configuration
@ConditionalOnProperty(name = "application.filestore.backend", havingValue = "mongoGridFs")
public class MongoGridFsBackingStoreConfig {

    @Bean
    @Qualifier("permanentBackingStore")
    BackingStore mongoGridFsPermanentFileStoreTemplate(MongoConverter mongoConverter, MongoDatabaseFactory dbFactory) {
        GridFsTemplate gridFsTemplate = new GridFsTemplate(dbFactory, mongoConverter, "fs.permanent");
        return new MongoGridFsBackingStore(gridFsTemplate);
    }

    @Bean
    @Qualifier("ephemeralBackingStore")
    BackingStore mongoGridFsEphemeralFileStoreTemplate(MongoConverter mongoConverter, MongoDatabaseFactory dbFactory) {
        GridFsTemplate gridFsTemplate = new GridFsTemplate(dbFactory, mongoConverter, "fs.ephemeral");
        return new MongoGridFsBackingStore(gridFsTemplate);
    }
}
