package org.hyperledger.bela.converter;

import com.google.common.base.Supplier;
import com.google.common.collect.Streams;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.plugin.services.BesuConfiguration;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageFactory;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.DatabaseMetadata;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfiguration;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBFactoryConfiguration;
import org.hyperledger.besu.plugin.services.storage.rocksdb.segmented.BelaRocksDBColumnarKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class RocksDBKeyValueStorageConverterFactory implements KeyValueStorageFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(RocksDBKeyValueStorageConverterFactory.class);
  private RocksDBConfiguration rocksDBConfiguration;
  private static SegmentedKeyValueStorage segmentedStorage;
  private final List<SegmentIdentifier> segments;
  private final RocksDBMetricsFactory rocksDBMetricsFactory;
  private static final Set<Integer> SUPPORTED_VERSIONS = Set.of(1, 2);


  private final Supplier<RocksDBFactoryConfiguration> configuration;


  public RocksDBKeyValueStorageConverterFactory(
      final Supplier<RocksDBFactoryConfiguration> configuration,
      final List<SegmentIdentifier> segments, final RocksDBMetricsFactory rocksDBMetricsFactory) {
    this.configuration = configuration;
    this.segments = segments;
    this.rocksDBMetricsFactory = rocksDBMetricsFactory;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public KeyValueStorage create(final SegmentIdentifier segment,
      final BesuConfiguration commonConfiguration, final MetricsSystem metricsSystem)
      throws StorageException {

    return new SegmentedKeyValueStorageAdapter(segment,
        create(segments, commonConfiguration, metricsSystem));
  }

  @Override
  public SegmentedKeyValueStorage create(List<SegmentIdentifier> requestedSegments,
      BesuConfiguration commonConfiguration, MetricsSystem metricsSystem) throws StorageException {
    if (requiresInit()) {
      init(commonConfiguration);
    }

    // create segmented storage for the distinct set of requested and detected segments
    var allSegments =
        Streams.concat(segments.stream(), requestedSegments.stream()).distinct().toList();

    if (segmentedStorage == null) {
      List<SegmentIdentifier> ignorableSegments = new ArrayList<>();
      segmentedStorage =
          new BelaRocksDBColumnarKeyValueStorage(rocksDBConfiguration, new ArrayList<>(allSegments),
              ignorableSegments, metricsSystem, rocksDBMetricsFactory);
    }
    return segmentedStorage;
  }


  @Override
  public boolean isSegmentIsolationSupported() {
    return false;
  }

  @Override
  public boolean isSnapshotIsolationSupported() {
    return false;
  }

  private void init(final BesuConfiguration commonConfiguration) {
    try {
      checkDatabaseVersion(commonConfiguration);
    } catch (final IOException e) {
      throw new StorageException("Failed to retrieve the RocksDB database meta version", e);
    }
    rocksDBConfiguration = RocksDBConfigurationBuilder
        .from(configuration.get())
        .databaseDir(commonConfiguration.getStoragePath())
        .build();
  }

  private boolean requiresInit() {
    return segmentedStorage == null;
  }

  private void checkDatabaseVersion(final BesuConfiguration commonConfiguration)
      throws IOException {
    final Path dataDir = commonConfiguration.getDataPath();
    final boolean databaseExists = commonConfiguration.getStoragePath().toFile().exists();
    final DatabaseMetadata databaseMetadata;

    if (databaseExists) {
      databaseMetadata = DatabaseMetadata.lookUpFrom(dataDir);
      LOG.info("Existing database detected at {}. Version {}", dataDir, databaseMetadata);
    } else {
      final String message = "No existing database detected at " + dataDir;
      LOG.error(message);
      throw new StorageException(message);
    }

    if (!SUPPORTED_VERSIONS.contains(databaseMetadata)) {
      final String message = "Unsupported RocksDB Metadata version of: " + databaseMetadata;
      LOG.error(message);
      throw new StorageException(message);
    }

  }

  @Override
  public void close() throws IOException {
    if (segmentedStorage != null) {
      segmentedStorage.close();
      segmentedStorage = null;
    }
  }
}
