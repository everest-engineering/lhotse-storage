package engineering.everest.starterkit.filestorage.config;

import engineering.everest.starterkit.filestorage.FileStore;
import engineering.everest.starterkit.filestorage.filestores.MongoGridFsFileStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.gridfs.GridFsTemplate;

@Configuration
@ConditionalOnProperty(name = "application.filestore.backend", havingValue = "mongoGridFs")
public class MongoGridFsFileStoreConfig {

    @Bean
    @Qualifier("permanentFileStore")
    FileStore mongoGridFsPermanentFileStoreTemplate(MongoConverter mongoConverter,
                                                    MongoDatabaseFactory dbFactory) {
        GridFsTemplate gridFsTemplate = new GridFsTemplate(dbFactory, mongoConverter, "fs.permanent");
        return new MongoGridFsFileStore(gridFsTemplate);
    }

    @Bean
    @Qualifier("ephemeralFileStore")
    FileStore mongoGridFsEphemeralFileStoreTemplate(MongoConverter mongoConverter,
                                                    MongoDatabaseFactory dbFactory) {
        GridFsTemplate gridFsTemplate = new GridFsTemplate(dbFactory, mongoConverter, "fs.ephemeral");
        return new MongoGridFsFileStore(gridFsTemplate);
    }

}
