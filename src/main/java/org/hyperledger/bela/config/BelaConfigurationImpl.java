package org.hyperledger.bela.config;

import java.io.IOException;
import java.nio.file.Path;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.plugin.services.BesuConfiguration;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.DatabaseMetadata;

public class BelaConfigurationImpl implements BesuConfiguration {

    private final Path storagePath;
    private final Path dataPath;

    public BelaConfigurationImpl(final Path dataPath, final Path storagePath) {
        this.dataPath = dataPath;
        this.storagePath = storagePath;
    }

    @Override
    public Path getStoragePath() {
        return storagePath;
    }

    @Override
    public Path getDataPath() {
        return dataPath;
    }

    @Override
    public DataStorageFormat getDatabaseFormat() {
      try {
        return DatabaseMetadata.lookUpFrom(dataPath).getVersionedStorageFormat().getFormat();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Wei getMinGasPrice() {
        return Wei.ZERO;
    }
}
