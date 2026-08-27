// SPDX-License-Identifier: MIT OR Apache-2.0

//! Tests for how the node reacts to the blocks its peers send.
//!
//! Whether a block *is* mutated is decided in `floresta-chain`, and is covered by the mutation
//! matrix in `Consensus`. What is tested here is the reaction: who gets banned, whether the block
//! is kept, and what happens to a user request that a mutated block interrupted.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Instant;

    use bitcoin::Block;
    use bitcoin::Network;
    use floresta_chain::ChainState;
    use floresta_chain::FlatChainStore;
    use tokio::sync::oneshot;
    use tokio::sync::oneshot::error::TryRecvError;

    use crate::node::ConnectionKind;
    use crate::node::InflightRequests;
    use crate::node::PeerStatus;
    use crate::node::UtreexoNode;
    use crate::node::sync_ctx::SyncNode;
    use crate::node_handle::NodeResponse;
    use crate::node_handle::UserRequest;
    use crate::p2p_wire::error::WireError;
    use crate::p2p_wire::tests::utils::PeerData;
    use crate::p2p_wire::tests::utils::build_node;
    use crate::p2p_wire::tests::utils::mark_peer_ready;
    use crate::p2p_wire::tests::utils::mutated_block_h7;
    use crate::p2p_wire::tests::utils::signet_blocks;
    use crate::p2p_wire::tests::utils::signet_headers;

    /// The peer that sends us the block under test.
    const SENDER: u32 = 0;
    /// A second, honest peer that a retry can fall back on.
    const OTHER: u32 = 1;

    type TestNode = UtreexoNode<Arc<ChainState<FlatChainStore>>, SyncNode>;

    /// Builds a node with `num_peers` ready peers, all able to serve blocks.
    fn node_with_peers(num_peers: usize) -> TestNode {
        let datadir = format!("./tmp-db/{}.blocks", rand::random::<u32>());
        let blocks = signet_blocks();

        let peers = vec![PeerData::new(Vec::new(), blocks, HashMap::new()); num_peers];
        let (mut node, _chain) = build_node(peers, false, Network::Signet, &datadir, 9);

        for peer in 0..num_peers as u32 {
            mark_peer_ready(&mut node, peer);
        }

        node
    }

    /// An honest signet block with a single coinbase transaction.
    fn honest_block() -> Block {
        let block_hash = signet_headers()[1].block_hash();

        signet_blocks().get(&block_hash).unwrap().clone()
    }

    /// Registers `block_hash` as inflight from [`SENDER`], as if we had just asked for it.
    fn expect_block(node: &mut TestNode, block_hash: bitcoin::BlockHash) {
        node.inflight.insert(
            InflightRequests::Blocks(block_hash),
            (SENDER, Instant::now()),
        );
    }

    /// Registers a pending user request for `block_hash`, returning the caller's end of it.
    fn expect_user_block(
        node: &mut TestNode,
        block_hash: bitcoin::BlockHash,
    ) -> oneshot::Receiver<NodeResponse> {
        let (tx, rx) = oneshot::channel();
        node.inflight_user_requests
            .insert(UserRequest::Block(block_hash), (SENDER, Instant::now(), tx));

        rx
    }

    #[tokio::test]
    async fn honest_block_is_kept_and_its_request_cleared() {
        let mut node = node_with_peers(1);
        let block = honest_block();
        let block_hash = block.block_hash();
        expect_block(&mut node, block_hash);

        node.request_block_proof(block, SENDER).unwrap();

        assert!(node.blocks.contains_key(&block_hash));
        assert!(
            !node
                .inflight
                .contains_key(&InflightRequests::Blocks(block_hash))
        );
        assert_eq!(node.peers.get(&SENDER).unwrap().state, PeerStatus::Ready);

        // Coinbase-only, so there are no previous outputs to prove and no proof is requested
        assert!(node.blocks.get(&block_hash).unwrap().aux_data.is_some());
        assert!(
            !node
                .inflight
                .contains_key(&InflightRequests::UtreexoProof(block_hash))
        );
    }

    #[tokio::test]
    async fn honest_block_answers_the_user_and_is_not_stored() {
        let mut node = node_with_peers(1);
        let block = honest_block();
        let block_hash = block.block_hash();
        expect_block(&mut node, block_hash);
        let response = expect_user_block(&mut node, block_hash);

        node.request_block_proof(block, SENDER).unwrap();

        match response.await.unwrap() {
            NodeResponse::Block(Some(block)) => assert_eq!(block.block_hash(), block_hash),
            other => panic!("expected the block back, got {other:?}"),
        }

        // The block was handed to the user, so the node doesn't keep a copy
        assert!(!node.blocks.contains_key(&block_hash));
        assert!(
            !node
                .inflight_user_requests
                .contains_key(&UserRequest::Block(block_hash))
        );
    }

    #[tokio::test]
    async fn mutated_block_bans_the_sender_and_is_discarded() {
        let mut node = node_with_peers(2);
        let block = mutated_block_h7();
        let block_hash = block.block_hash();
        expect_block(&mut node, block_hash);

        let result = node.request_block_proof(block, SENDER);

        assert!(matches!(result, Err(WireError::PeerMisbehaving)));
        assert_eq!(node.peers.get(&SENDER).unwrap().state, PeerStatus::Banned);

        // The block is dropped rather than kept, and no proof is requested for it. The regular
        // re-request machinery picks it up again on the next maintenance tick.
        assert!(!node.blocks.contains_key(&block_hash));
        assert!(
            !node
                .inflight
                .contains_key(&InflightRequests::UtreexoProof(block_hash))
        );
    }

    #[tokio::test]
    async fn mutated_user_block_is_retried_with_a_different_peer() {
        let mut node = node_with_peers(2);
        let block = mutated_block_h7();
        let block_hash = block.block_hash();
        expect_block(&mut node, block_hash);
        let mut response = expect_user_block(&mut node, block_hash);

        node.request_block_proof(block, SENDER).unwrap();

        assert_eq!(node.peers.get(&SENDER).unwrap().state, PeerStatus::Banned);

        // The retry goes to the honest peer, never back to the one that just lied
        let (retry_peer, _) = node
            .inflight
            .get(&InflightRequests::Blocks(block_hash))
            .expect("the retry is inflight");
        assert_eq!(*retry_peer, OTHER);

        // The user is still waiting, and hasn't been given the mutated block
        assert!(
            node.inflight_user_requests
                .contains_key(&UserRequest::Block(block_hash))
        );
        assert!(matches!(response.try_recv(), Err(TryRecvError::Empty)));
    }

    /// A mutated block must never leave a user request parked with nothing to complete it:
    /// nothing sweeps `inflight_user_requests` for timeouts, so the caller would wait forever.
    #[tokio::test]
    async fn mutated_user_block_fails_the_request_when_no_peer_is_left() {
        let mut node = node_with_peers(1);
        let block = mutated_block_h7();
        let block_hash = block.block_hash();
        expect_block(&mut node, block_hash);
        let mut response = expect_user_block(&mut node, block_hash);

        let result = node.request_block_proof(block, SENDER);

        assert!(matches!(result, Err(WireError::NoPeersAvailable)));
        assert!(
            !node
                .inflight
                .contains_key(&InflightRequests::Blocks(block_hash))
        );

        // Dropping the request closes the channel, which wakes the caller with an error
        assert!(
            !node
                .inflight_user_requests
                .contains_key(&UserRequest::Block(block_hash))
        );
        assert!(matches!(response.try_recv(), Err(TryRecvError::Closed)));
    }

    /// Manual peers are exempt from bans, so banning alone won't keep a lying one out of the
    /// candidate set. Without an explicit exclusion the retry lands straight back on it, and the
    /// two sides ping-pong with no back-off.
    #[tokio::test]
    async fn mutated_user_block_is_not_retried_with_an_unbannable_peer() {
        let mut node = node_with_peers(2);
        node.peers.get_mut(&SENDER).unwrap().kind = ConnectionKind::Manual;

        let block = mutated_block_h7();
        let block_hash = block.block_hash();
        expect_block(&mut node, block_hash);
        let _response = expect_user_block(&mut node, block_hash);

        node.request_block_proof(block, SENDER).unwrap();

        // Still `Ready`, because manual peers don't get banned
        assert_eq!(node.peers.get(&SENDER).unwrap().state, PeerStatus::Ready);

        let (retry_peer, _) = node
            .inflight
            .get(&InflightRequests::Blocks(block_hash))
            .expect("the retry is inflight");
        assert_eq!(*retry_peer, OTHER);
    }
}
