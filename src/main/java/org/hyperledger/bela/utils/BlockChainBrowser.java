/*
 *
 *  * Copyright Hyperledger Besu Contributors.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  * the License. You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  * specific language governing permissions and limitations under the License.
 *  *
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.hyperledger.bela.utils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Optional;
import com.googlecode.lanterna.gui2.Component;
import com.googlecode.lanterna.gui2.Panel;
import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.bela.components.BlockPanel;
import org.hyperledger.bela.components.BelaComponent;
import org.hyperledger.bela.components.SummaryPanel;
import org.hyperledger.bela.model.BlockResult;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.bonsai.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.chain.BlockchainStorage;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDbSegmentIdentifier;

public class BlockChainBrowser {

    static final Bytes BLOCK_HEADER_PREFIX = Bytes.of(2);
    private static final Bytes BLOCK_BODY_PREFIX = Bytes.of(3);
    private static final Bytes BLOCK_HASH_PREFIX = Bytes.of(5);


    private final BonsaiWorldStateKeyValueStorage worldStateStorage;
    private final DefaultBlockchain blockchain;
    private Optional<BlockResult> blockResult;
    private BlockPanel blockPanel;
    private SummaryPanel summaryPanel;


    public BlockChainBrowser(
            final DefaultBlockchain blockchain,
            final BonsaiWorldStateKeyValueStorage worldStateStorage) {
        this.blockchain = blockchain;
        this.worldStateStorage = worldStateStorage;
        //fixme
        this.blockResult = getChainHead();
        blockResult.ifPresent(result -> this.blockPanel = new BlockPanel(result));
        blockResult.ifPresent(result -> this.summaryPanel = new SummaryPanel(
                result.getStateRoot(),
                String.valueOf(result.getNumber()), result.getHash()));

    }


    public static BlockChainBrowser fromBlockChainContext(final BlockChainContext blockChainContext) {
        return new BlockChainBrowser((DefaultBlockchain) blockChainContext.getBlockchain()/*, worldStateArchive*/, blockChainContext.getWorldStateStorage());
    }


    public BelaComponent<Panel> blockPanel() {
        return blockPanel;
    }

    public BelaComponent<? extends Component> showSummaryPanel() {
        return summaryPanel;
    }

    public Optional<BlockResult> getChainHead() {
        return getBlockByHash(blockchain.getChainHead().getHash());
    }

    public BlockChainBrowser moveBackward() {
        if (blockResult.isPresent()) {
            blockResult = getBlockByHash(Hash.fromHexString(blockResult.get().getParentHash()));
        } else {
            blockResult = getChainHead();
        }
        updatePanels(blockResult);
        return this;
    }

    private void updatePanels(final Optional<BlockResult> blockResult) {
        this.blockResult.ifPresent(result -> blockPanel.updateWithBlock(result));
        this.blockResult.ifPresent(result -> this.summaryPanel.updateWith(blockchain.getChainHeadBlock()));
    }

    public BlockChainBrowser moveForward() {
        if (blockResult.isPresent()) {
            blockResult = getBlockByNumber(blockResult.get().getNumber() + 1);
        } else {
            blockResult = getBlockByNumber(0);
        }
        updatePanels(blockResult);
        return this;
    }

    public Optional<BlockResult> getBlockByNumber(final long blockNumber) {
        return blockchain.getBlockHashByNumber(blockNumber)
                .flatMap(this::getBlockByHash);
    }

    public Optional<BlockResult> getBlockByHash(final Hash blockHash) {
        return blockchain.getBlockHeader(blockHash)
                .flatMap(header -> blockchain.getBlockBody(header.getHash())
                        .map(body -> new Block(header, body)))
                .map(block -> new BlockResult(
                        block,
                        blockchain.getTotalDifficultyByHash(block.getHash()))
                );
    }

    public String getBlockHash() {
        return blockResult.get().getHash();
    }

    public void rollHead() {
        if (blockResult.isPresent()) {
            blockchain.rewindToBlock(Hash.fromHexString(blockResult.get().getHash()));
            updateSummary();
        }
    }

    public void deleteHash(Hash blockHash) {
        try {
            final Field trieLogField = BonsaiWorldStateKeyValueStorage.class.getDeclaredField("trieLogStorage");
            trieLogField.setAccessible(true);
            final Field blockchainStorageField = DefaultBlockchain.class.getDeclaredField("blockchainStorage");
            blockchainStorageField.setAccessible(true);
            var blockchainStorage = (KeyValueStoragePrefixedKeyBlockchainStorage) blockchainStorageField.get(blockchain);
            var updater = blockchainStorage.updater();
            final Method remove = KeyValueStoragePrefixedKeyBlockchainStorage.Updater.class.getDeclaredMethod("remove", Bytes.class, Bytes.class);
            remove.setAccessible(true);

            remove.invoke(updater, BLOCK_HASH_PREFIX, blockHash);
            remove.invoke(updater, BLOCK_HEADER_PREFIX, blockHash);
            remove.invoke(updater, BLOCK_BODY_PREFIX, blockHash);
            updater.commit();

            var trieLogStorage = (KeyValueStorage) trieLogField.get(worldStateStorage);
            trieLogStorage.tryDelete(blockHash.toArrayUnsafe());

        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
        updateSummary();
    }

    private void updateSummary() {
        final Block chainHeadBlock = blockchain.getChainHeadBlock();
        summaryPanel.updateWith(chainHeadBlock);
    }

    public void moveByHash(final Hash hash) {
        blockResult = getBlockByHash(hash);
        updatePanels(blockResult);
    }

    public void moveByNumber(final long number) {
        blockResult = getBlockByNumber(number);
        updatePanels(blockResult);
    }

    public BlockChainBrowser moveToHead() {
        blockResult = getChainHead();
        updatePanels(blockResult);
        return this;
    }

    public BlockChainBrowser moveToStart() {
        blockResult = getBlockByNumber(0);
        updatePanels(blockResult);
        return this;
    }

    public boolean hasTransactions() {
        return blockResult.map(r -> !r.getTransactions().isEmpty()).orElse(false);
    }
}
