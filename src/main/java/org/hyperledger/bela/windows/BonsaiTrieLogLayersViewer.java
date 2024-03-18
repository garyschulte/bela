package org.hyperledger.bela.windows;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import com.googlecode.lanterna.gui2.Direction;
import com.googlecode.lanterna.gui2.LinearLayout;
import com.googlecode.lanterna.gui2.Panel;
import com.googlecode.lanterna.gui2.WindowBasedTextGUI;
import com.googlecode.lanterna.gui2.dialogs.TextInputDialog;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.bela.components.KeyControls;
import org.hyperledger.bela.components.bonsai.BonsaiNode;
import org.hyperledger.bela.components.bonsai.BonsaiTrieLogView;
import org.hyperledger.bela.components.bonsai.RootTrieLogSearchResult;
import org.hyperledger.bela.components.bonsai.queries.BonsaiTrieQuery;
import org.hyperledger.bela.components.bonsai.queries.TrieQueryValidator;
import org.hyperledger.bela.dialogs.BelaDialog;
import org.hyperledger.bela.dialogs.ProgressBarPopup;
import org.hyperledger.bela.utils.BlockChainContext;
import org.hyperledger.bela.utils.BlockChainContextFactory;
import org.hyperledger.bela.utils.StorageProviderFactory;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.ChainHead;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.bonsai.cache.CachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.bonsai.worldview.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;

import static kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory.getLogger;
import static org.hyperledger.bela.windows.Constants.KEY_HEAD;
import static org.hyperledger.bela.windows.Constants.KEY_LOG;
import static org.hyperledger.bela.windows.Constants.KEY_LOOKUP_BY_HASH;
import static org.hyperledger.bela.windows.Constants.KEY_QUERY;
import static org.hyperledger.bela.windows.Constants.KEY_ROLL_BACKWARD;
import static org.hyperledger.bela.windows.Constants.KEY_ROLL_FORWARD;
import static org.hyperledger.bela.windows.Constants.KEY_SHOW_ALL;

public class BonsaiTrieLogLayersViewer extends AbstractBelaWindow {
    private static final LambdaLogger log = getLogger(BonsaiTrieLogLayersViewer.class);

    private final WindowBasedTextGUI gui;
    private final StorageProviderFactory storageProviderFactory;

    private final BonsaiTrieLogView view;

    public BonsaiTrieLogLayersViewer(final WindowBasedTextGUI gui, final StorageProviderFactory storageProviderFactory) {

        this.gui = gui;
        this.storageProviderFactory = storageProviderFactory;

        this.view = new BonsaiTrieLogView(storageProviderFactory);
    }

    @Override
    public String label() {
        return "Bonsai Trie Log Layers Viewer";
    }

    @Override
    public MenuGroup group() {
        return MenuGroup.DATABASE;
    }

    @Override
    public KeyControls createControls() {
        return new KeyControls()
                .addControl("By Hash", KEY_LOOKUP_BY_HASH, this::lookupByHash)
                .addControl("To Log", KEY_LOG, view::logCurrent)
                .addControl("Head", KEY_HEAD, this::lookupByChainHead)
                .addControl("Query", KEY_QUERY, this::query)
                .addControl("All", KEY_SHOW_ALL, this::showAll)
                .addControl("Roll Forward", KEY_ROLL_FORWARD, this::rollForward)
                .addControl("Roll Backward", KEY_ROLL_BACKWARD, this::rollBackward);
    }

    private void query() {
        BelaDialog.showDelegateListDialog(gui, "Select a query", Arrays.asList(BonsaiTrieQuery.values()), BonsaiTrieQuery::getName,
                this::executeQuery);
    }

    private void executeQuery(final BonsaiTrieQuery query) {
        try {
            executeQuery(query.createValidator(gui));
        } catch (Exception e) {
            log.error("There was an error", e);
            BelaDialog.showException(gui, e);
        }

    }

    public void executeQuery(final TrieQueryValidator validator) {
        final StorageProvider provider = storageProviderFactory.createProvider();
        final long estimate = SegmentManipulationWindow.accessLongPropertyForSegment(provider, KeyValueSegmentIdentifier.TRIE_LOG_STORAGE, LongRocksDbProperty.ROCKSDB_ESTIMATE_NUM_KEYS);
        final ProgressBarPopup popup = ProgressBarPopup.showPopup(gui, "Searching", (int) estimate);
        final KeyValueStorage storage = provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE);
        final List<BonsaiNode> results = storage.streamKeys().map(entry -> Hash.wrap(Bytes32.wrap(entry)))
                .map(hash -> {
                    popup.increment();
                    return BonsaiTrieLogView.getTrieLog(storage, hash);
                })
                .flatMap(Optional::stream)
                .filter(validator::validate)
                .map(trieLogLayer -> new RootTrieLogSearchResult(storage, trieLogLayer.getBlockHash()))
                .collect(Collectors.toList());
        view.shoResults(storage, results);
        popup.close();
    }

    private void showAll() {
        view.showAllTries();
    }

    @Override
    public Panel createMainPanel() {
        Panel panel = new Panel(new LinearLayout(Direction.VERTICAL));

        panel.addComponent(view.createComponent());

        return panel;
    }


    private void rollForward() {
        try {
            final BonsaiWorldStateUpdateAccumulator updater = getBonsaiWorldStateAccumulator();
            updater.rollForward(view.getLayer());
            updater.commit();
            storageProviderFactory.close();
        } catch (Exception e) {
            BelaDialog.showException(gui, e);
        }
    }

    private void rollBackward() {
        try {
            final BonsaiWorldStateUpdateAccumulator updater = getBonsaiWorldStateAccumulator();
            updater.rollBack(view.getLayer());
            updater.commit();
            storageProviderFactory.close();
        } catch (Exception e) {
            BelaDialog.showException(gui, e);
        }

    }

    private BonsaiWorldStateUpdateAccumulator getBonsaiWorldStateAccumulator() {
        final StorageProvider provider = storageProviderFactory.createProvider();
        final BlockChainContext blockChainContext = BlockChainContextFactory.createBlockChainContext(provider);

        final NoOpMetricsSystem noOpMetricsSystem = new NoOpMetricsSystem();
        final BonsaiWorldStateKeyValueStorage storage = new BonsaiWorldStateKeyValueStorage(
            provider, noOpMetricsSystem, DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
        final BonsaiWorldStateProvider archive = new BonsaiWorldStateProvider(
            storage,
            blockChainContext.getBlockchain(),
            Optional.empty(),
            new CachedMerkleTrieLoader(noOpMetricsSystem),
            null,
            EvmConfiguration.DEFAULT);

        return (BonsaiWorldStateUpdateAccumulator) archive.getMutable().updater();

    }


    private void lookupByChainHead() {
        final BlockChainContext blockChainContext = BlockChainContextFactory.createBlockChainContext(storageProviderFactory.createProvider());
        final ChainHead chainHead = blockChainContext.getBlockchain().getChainHead();
        updateTrieFromHash(chainHead.getHash());

    }

    private void lookupByHash() {
        final BlockChainContext blockChainContext = BlockChainContextFactory.createBlockChainContext(storageProviderFactory.createProvider());
        final ChainHead chainHead = blockChainContext.getBlockchain().getChainHead();
        final String s = TextInputDialog.showDialog(gui, "Enter Block Hash", "Hash", chainHead.getHash().toHexString());
        if (s == null) {
            return;
        }
        try {
            final Hash hash = Hash.fromHexString(s);
            updateTrieFromHash(hash);
        } catch (Exception e) {
            log.error("There was an error when showing Trie", e);
            BelaDialog.showException(gui, e);
        }
    }

    private void updateTrieFromHash(final Hash hash) {
        view.updateFromHash(hash);
    }


}
