package org.hyperledger.bela.converter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import com.google.common.base.Supplier;
import org.hyperledger.besu.plugin.services.BesuConfiguration;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageFactory;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.DatabaseMetadata;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfiguration;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBFactoryConfiguration;
import org.hyperledger.besu.plugin.services.storage.rocksdb.segmented.RocksDBColumnarKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBKeyValueStorageConverterFactory implements KeyValueStorageFactory {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBKeyValueStorageConverterFactory.class);
    private RocksDBConfiguration rocksDBConfiguration;
    private SegmentedKeyValueStorage<?> segmentedStorage;
    private final List<SegmentIdentifier> segments;
    private final RocksDBMetricsFactory rocksDBMetricsFactory;
    private static final Set<Integer> SUPPORTED_VERSIONS = Set.of(1, 2);

    private final boolean strictColumnFamilyDefinitions;

    private final Supplier<RocksDBFactoryConfiguration> configuration;


    public RocksDBKeyValueStorageConverterFactory(
        final Supplier<RocksDBFactoryConfiguration> configuration,
        final List<SegmentIdentifier> segments,
        final RocksDBMetricsFactory rocksDBMetricsFactory) {
        this(configuration, segments, rocksDBMetricsFactory, false);
    }

    public RocksDBKeyValueStorageConverterFactory(
            final Supplier<RocksDBFactoryConfiguration> configuration,
            final List<SegmentIdentifier> segments,
            final RocksDBMetricsFactory rocksDBMetricsFactory,
            final boolean openWithAllColumnFamilies) {
        this.configuration = configuration;
        this.segments = segments;
        this.rocksDBMetricsFactory = rocksDBMetricsFactory;
        this.strictColumnFamilyDefinitions = openWithAllColumnFamilies;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public KeyValueStorage create(
            final SegmentIdentifier segment,
            final BesuConfiguration commonConfiguration,
            final MetricsSystem metricsSystem)
            throws StorageException {

        if (requiresInit()) {
            init(commonConfiguration);
        }

        try {
            int dbVersion = readDatabaseVersion(commonConfiguration);
            if (segmentedStorage == null) {
                final Map<SegmentIdentifier, Boolean> segmentsForVersion =
                    segments.stream()
                        .map(seg -> new AbstractMap.SimpleEntry<>(seg, seg.includeInDatabaseVersion(dbVersion)))
                        .collect(Collectors.toMap(
                            e -> e.getKey(), e -> e.getValue()
                        ));

                // if we are in "strict mode" check if the db column families match the metadata definition
                if (strictColumnFamilyDefinitions) {

                    // if we have segments that exist, but shouldn't, repair by dropping the ones that shouldn't exist
                    if (segmentsForVersion.values().stream().filter(v -> !v).findAny().orElse(false)) {

                        var unfilteredStorage = new RocksDBColumnarKeyValueStorage(
                            rocksDBConfiguration,
                            segmentsForVersion.entrySet().stream()
                                .map(Map.Entry::getKey)
                                .collect(Collectors.toList()),
                            metricsSystem,
                            rocksDBMetricsFactory);

                        segmentsForVersion.entrySet().stream()
                            .filter(e -> !e.getValue())
                            .forEach(seg -> unfilteredStorage.drop(seg));
                        unfilteredStorage.close();

                    }
                }


                // create the segmented version
                segmentedStorage =
                    new RocksDBColumnarKeyValueStorage(
                        rocksDBConfiguration,
                        segmentsForVersion.entrySet().stream()
                            .filter(e -> e.getValue())
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toList()),
                        metricsSystem,
                        rocksDBMetricsFactory);
            }
            return new SegmentedKeyValueStorageAdapter<>(segment, segmentedStorage);
        } catch(IOException ioe) {
            //boom
            throw new RuntimeException(ioe);
        }
    }


    @Override
    public boolean isSegmentIsolationSupported() {
        return false;
    }

    private void init(final BesuConfiguration commonConfiguration) {
        try {
            checkDatabaseVersion(commonConfiguration);
        } catch (final IOException e) {
            throw new StorageException("Failed to retrieve the RocksDB database meta version", e);
        }
        rocksDBConfiguration =
                RocksDBConfigurationBuilder.from(configuration.get())
                        .databaseDir(commonConfiguration.getStoragePath())
                        .build();
    }

    private boolean requiresInit() {
        return segmentedStorage == null;
    }

    private void checkDatabaseVersion(final BesuConfiguration commonConfiguration) throws IOException {
        final Path dataDir = commonConfiguration.getDataPath();
        final boolean databaseExists = commonConfiguration.getStoragePath().toFile().exists();
        final int databaseVersion;

        if (databaseExists) {
            databaseVersion = DatabaseMetadata.lookUpFrom(dataDir).getVersion();
            LOG.info("Existing database detected at {}. Version {}", dataDir, databaseVersion);
        } else {
            final String message = "No existing database detected at " + dataDir;
            LOG.error(message);
            throw new StorageException(message);
        }

        if (!SUPPORTED_VERSIONS.contains(databaseVersion)) {
            final String message = "Unsupported RocksDB Metadata version of: " + databaseVersion;
            LOG.error(message);
            throw new StorageException(message);
        }

    }

    //TODO: make this class extend RocksDBKeyValueStorageFactory, and make readDatabaseVersion protected rather than private
    private int readDatabaseVersion(final BesuConfiguration commonConfiguration) throws IOException {
        final Path dataDir = commonConfiguration.getDataPath();
        final boolean databaseExists = commonConfiguration.getStoragePath().toFile().exists();
        final int databaseVersion;
        if (databaseExists) {
            databaseVersion = DatabaseMetadata.lookUpFrom(dataDir).getVersion();
            LOG.info("Existing database detected at {}. Version {}", dataDir, databaseVersion);
        } else {
            databaseVersion = commonConfiguration.getDatabaseVersion();
            LOG.info("No existing database detected at {}. Using version {}", dataDir, databaseVersion);
            Files.createDirectories(dataDir);
            new DatabaseMetadata(databaseVersion).writeToDirectory(dataDir);
        }

        if (!SUPPORTED_VERSIONS.contains(databaseVersion)) {
            final String message = "Unsupported RocksDB Metadata version of: " + databaseVersion;
            LOG.error(message);
            throw new StorageException(message);
        }

        return databaseVersion;
    }


    @Override
    public void close() throws IOException {
        if (segmentedStorage != null) {
            segmentedStorage.close();
        }
    }
}
