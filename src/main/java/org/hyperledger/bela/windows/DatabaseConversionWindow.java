package org.hyperledger.bela.windows;

import com.googlecode.lanterna.TerminalSize;
import com.googlecode.lanterna.gui2.Label;
import com.googlecode.lanterna.gui2.LinearLayout;
import com.googlecode.lanterna.gui2.Panel;
import com.googlecode.lanterna.gui2.TextBox;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.bela.components.KeyControls;
import org.hyperledger.bela.converter.DatabaseConverter;
import org.hyperledger.bela.utils.StorageProviderFactory;
import org.hyperledger.bela.utils.bonsai.BonsaiListener;
import org.hyperledger.bela.utils.bonsai.BonsaiTraversalTrieType;
import org.hyperledger.besu.datatypes.Hash;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory.getLogger;
import static org.hyperledger.bela.windows.Constants.KEY_CONVERT_TO_BONSAI;
import static org.hyperledger.bela.windows.Constants.KEY_CONVERT_TO_FOREST;

public class DatabaseConversionWindow extends AbstractBelaWindow implements BonsaiListener {
    private static final LambdaLogger log = getLogger(DatabaseConversionWindow.class);

    private final StorageProviderFactory storageProviderFactory;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final Label runningLabel = new Label("Not Running...");
    private final Label counterLabel = new Label("0");
    private final TextBox logTextBox = new TextBox(new TerminalSize(80, 7));
    AtomicInteger visited = new AtomicInteger(0);
    private Future<?> execution;

    public DatabaseConversionWindow(final StorageProviderFactory storageProviderFactory) {
        this.storageProviderFactory = storageProviderFactory;
    }

    @Override
    public String label() {
        return "Database Storage Format Converter";
    }

    @Override
    public MenuGroup group() {
        return MenuGroup.DATABASE;
    }


    @Override
    public KeyControls createControls() {
        return new KeyControls()
                .addControl("Convert to Forest", KEY_CONVERT_TO_FOREST, this::convertToForest)
                .addControl("Convert to Bonsai", KEY_CONVERT_TO_BONSAI, this::convertToBonsai);
    }

    @Override
    public Panel createMainPanel() {
        Panel panel = new Panel(new LinearLayout());

        panel.addComponent(runningLabel);
        panel.addComponent(counterLabel);
        panel.addComponent(logTextBox);


        return panel;
    }

    private void convertToBonsai() {
        if (execution == null) {
            visited.set(0);
            execution = executorService.submit(() -> {
                new DatabaseConverter(storageProviderFactory.createProvider(), this).convertToBonsai();
                runningLabel.setText("Converting Worldstate to Bonsai");

            });
            runningLabel.setText("Running...");
        }
    }

    private void convertToForest() {
        if (execution == null) {
            visited.set(0);
            execution = executorService.submit(() -> {
                new DatabaseConverter(storageProviderFactory.createProvider(), this).convertToForest();
                runningLabel.setText("Converting Worldstate to Forest");

            });
            runningLabel.setText("Running...");
        }
    }

    @Override
    public void root(final Bytes32 hash) {
        logTextBox.addLine("Working with root " + hash);
    }

    @Override
    public void missingCodeHash(final Hash codeHash, final Hash accountHash) {
        logTextBox.addLine(String.format("missing code hash %s for account %s", codeHash, accountHash));

    }

    @Override
    public void invalidCode(final Hash accountHash, final Hash codeHash, final Hash foundCodeHash) {
        logTextBox.addLine(String.format("invalid code for account %s (expected %s and found %s)", accountHash, codeHash, foundCodeHash));
    }

    @Override
    public void missingValueForNode(final Bytes32 hash) {
        logTextBox.addLine("Missing value for node " + hash.toHexString());

    }

    @Override
    public void visited(final BonsaiTraversalTrieType type) {
        counterLabel.setText(String.valueOf(visited.incrementAndGet()));
    }

    @Override
    public void missingAccountTrieForHash(final Bytes32 hash, final Bytes location) {
        logTextBox.addLine(String.format("missing account trie node for hash %s and location %s", hash, location));

    }

    @Override
    public void invalidAccountTrieForHash(final Bytes32 hash, final Bytes location, final Hash foundHashNode) {
        logTextBox.addLine(String.format("invalid account trie node for hash %s and location %s (found %s)", hash, location, foundHashNode));
    }

    @Override
    public void missingStorageTrieForHash(final Bytes32 hash, final Bytes location) {
        logTextBox.addLine(String.format("missing storage trie node for hash %s and location %s", hash, location));
    }

    @Override
    public void invalidStorageTrieForHash(final Bytes32 accountHash, final Bytes32 hash, final Bytes location, final Hash foundHashNode) {
        logTextBox.addLine(String.format("invalid storage trie node for account %s hash %s and location %s (found %s)",accountHash, hash, location, foundHashNode));
    }

    @Override
    public void differentDataInFlatDatabaseForAccount(final Hash accountHash) {
        logTextBox.addLine(String.format("inconsistent data in flat database for account %s", accountHash));
    }

    @Override
    public void differentDataInFlatDatabaseForStorage(final Bytes32 accountHash, final Bytes32 slotHash) {
        logTextBox.addLine(String.format("inconsistent data in flat database for account %s on slot %s", accountHash, slotHash));
    }
}
