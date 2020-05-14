package engineering.everest.starterkit.filestorage.filestores;

import engineering.everest.starterkit.filestorage.FileStore;
import engineering.everest.starterkit.filestorage.InputStreamOfKnownLength;
import engineering.everest.starterkit.filestorage.NativeStorageType;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.gridfs.GridFsTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import static engineering.everest.starterkit.filestorage.NativeStorageType.MONGO_GRID_FS;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class MongoGridFsFileStore implements FileStore {

    private final GridFsTemplate gridFs;

    public MongoGridFsFileStore(GridFsTemplate gridFs) {
        this.gridFs = gridFs;
    }

    @Override
    public String uploadStream(InputStream inputStream, String fileName) {
        ObjectId mongoObjectId = gridFs.store(inputStream, fileName);
        return mongoObjectId.toHexString();
    }

    @Override
    public String uploadStream(InputStream inputStream, String fileName, long ignored) {
        ObjectId mongoObjectId = gridFs.store(inputStream, fileName);
        return mongoObjectId.toHexString();
    }

    @Override
    public void delete(String fileIdentifier) {
        gridFs.delete(query(where("_id").is(new ObjectId(fileIdentifier))));
    }

    @Override
    public InputStreamOfKnownLength downloadAsStream(String fileIdentifier) throws IOException {
        var gridFSFile = gridFs.findOne(new Query(where("_id").is(fileIdentifier)));
        if (gridFSFile == null) {
            throw new RuntimeException("Unable to retrieve file " + fileIdentifier);
        }
        return new InputStreamOfKnownLength(gridFs.getResource(gridFSFile).getInputStream(), gridFSFile.getLength());
    }

    @Override
    public NativeStorageType nativeStorageType() {
        return MONGO_GRID_FS;
    }

    @Override
    public void deleteFiles(Set<String> fileIdentifiers) {
        gridFs.delete(query(where("_id").in(fileIdentifiers)));
    }
}
