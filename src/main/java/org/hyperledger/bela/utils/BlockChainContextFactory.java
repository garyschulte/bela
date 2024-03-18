package org.hyperledger.bela.utils;

import org.hyperledger.bela.utils.ConsensusDetector.CONSENSUS_TYPE;
import org.hyperledger.besu.consensus.common.bft.BftBlockHeaderFunctions;
import org.hyperledger.besu.consensus.ibft.IbftExtraDataCodec;
import org.hyperledger.besu.consensus.qbft.QbftExtraDataCodec;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.chain.VariablesStorage;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.bonsai.cache.CachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;

import java.util.Optional;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.BLOCKCHAIN;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.VARIABLES;

public class BlockChainContextFactory {

  public static BlockChainContext createBlockChainContext(final StorageProvider storageProvider) {
    //init
    final KeyValueStorage keyValueStorage =
        storageProvider.getStorageBySegmentIdentifier(BLOCKCHAIN);
    final VariablesStorage variablesStorage =
        new VariablesKeyValueStorage(storageProvider.getStorageBySegmentIdentifier(VARIABLES));
    final CONSENSUS_TYPE consensusType =
        ConsensusDetector.detectConsensusMechanism(keyValueStorage, variablesStorage);
    final BlockHeaderFunctions blockHeaderFunction = switch (consensusType) {
      case IBFT2 -> BftBlockHeaderFunctions.forOnchainBlock(new IbftExtraDataCodec());
      case QBFT -> BftBlockHeaderFunctions.forOnchainBlock(new QbftExtraDataCodec());
      default -> new MainnetBlockHeaderFunctions();
    };

    final NoOpMetricsSystem noOpMetricsSystem = new NoOpMetricsSystem();
    var blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(keyValueStorage, variablesStorage,
            blockHeaderFunction);
    MutableBlockchain blockchain =
        (MutableBlockchain) DefaultBlockchain.create(blockchainStorage, new NoOpMetricsSystem(),
            0L);
    //TODO: we assume bonsai here but we should check the database metadata and load the appropriate worldstate
    var worldStateStorage = new BonsaiWorldStateKeyValueStorage(storageProvider, noOpMetricsSystem,
        DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
    var worldStateArchive =
        new BonsaiWorldStateProvider(worldStateStorage, blockchain, Optional.empty(),
            new CachedMerkleTrieLoader(noOpMetricsSystem), null, EvmConfiguration.DEFAULT);

    return new BlockChainContext(blockchain, worldStateStorage, worldStateArchive);
  }


}
