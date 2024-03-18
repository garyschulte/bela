package org.hyperledger.bela.components.bonsai.queries;


import org.hyperledger.besu.ethereum.trie.bonsai.trielog.TrieLogLayer;

public interface TrieQueryValidator {
    boolean validate(TrieLogLayer layer);
}
