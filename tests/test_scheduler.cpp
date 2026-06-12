#include "daemon/scheduler.hpp"

#include <iostream>
#include <tuple>
#include <vector>

using namespace thinbt;

static int fail(const char* msg) {
    std::cerr << "[FAIL] " << msg << std::endl;
    return 1;
}

int main() {
    std::vector<std::tuple<uint32_t, uint32_t, uint32_t, uint32_t>> requests;

    Scheduler sched;
    sched.init(
        1,
        1000,
        [&](uint32_t peer_slot, uint32_t chunk_idx, uint32_t begin, uint32_t length) {
            requests.emplace_back(peer_slot, chunk_idx, begin, length);
        },
        [](uint32_t) {},
        []() {});
    sched.set_chunk_sizes({SUB_BLOCK_SIZE * 2});

    sched.on_peer_added(1, 1000);
    sched.on_peer_speed(1, 2500);
    if (auto* slot = sched.peer_slot(1)) {
        if (slot->link_speed_reported != 2500 || slot->link_speed_mbps != 2500) {
            return fail("scheduler must refresh peer speed after handshake completes");
        }
    } else {
        return fail("peer slot should exist after adding a peer");
    }
    sched.on_bitfield(1, {true});
    sched.on_choke_change(1, false);
    sched.tick();

    if (requests.size() != 2) return fail("initial peer should receive both sub-block requests");

    requests.clear();

    Scheduler diversified_sched;
    diversified_sched.init(
        32,
        1000,
        [&](uint32_t peer_slot, uint32_t chunk_idx, uint32_t begin, uint32_t length) {
            requests.emplace_back(peer_slot, chunk_idx, begin, length);
        },
        [](uint32_t) {},
        []() {});
    diversified_sched.set_chunk_sizes(std::vector<uint32_t>(32, SUB_BLOCK_SIZE));
    diversified_sched.randomize_piece_order(12345);
    diversified_sched.on_peer_added(9, 1000);
    diversified_sched.on_bitfield(9, std::vector<bool>(32, true));
    diversified_sched.on_choke_change(9, false);
    diversified_sched.tick();

    bool sequential = requests.size() >= 8;
    for (size_t i = 0; i < 8 && i < requests.size(); ++i) {
        if (std::get<1>(requests[i]) != i) {
            sequential = false;
            break;
        }
    }
    if (sequential) {
        return fail("scheduler should support randomized initial piece order so concurrent downloaders do not mirror the same seed chunks");
    }

    requests.clear();

    sched.on_peer_removed(1);
    sched.on_peer_added(2, 1000);
    sched.on_bitfield(2, {true});
    sched.on_choke_change(2, false);
    sched.tick();

    if (requests.size() != 2) {
        return fail("disconnecting a peer must release its in-flight sub-blocks for rescheduling");
    }

    requests.clear();

    Scheduler swarm_sched;
    swarm_sched.init(
        2048,
        1000,
        [&](uint32_t peer_slot, uint32_t chunk_idx, uint32_t begin, uint32_t length) {
            requests.emplace_back(peer_slot, chunk_idx, begin, length);
        },
        [](uint32_t) {},
        []() {});
    swarm_sched.set_chunk_sizes(std::vector<uint32_t>(2048, SUB_BLOCK_SIZE));

    std::vector<bool> full_seed(2048, true);
    std::vector<bool> leecher_chunks(2048, false);
    for (uint32_t i = 1536; i < 2048; ++i)
        leecher_chunks[i] = true;

    swarm_sched.on_peer_added(1, 1000);
    swarm_sched.on_bitfield(1, full_seed);
    swarm_sched.on_choke_change(1, false);

    swarm_sched.on_peer_added(2, 1000);
    swarm_sched.on_bitfield(2, leecher_chunks);
    swarm_sched.on_choke_change(2, false);

    swarm_sched.tick();

    bool requested_from_leecher = false;
    for (const auto& req : requests) {
        if (std::get<0>(req) == 2 && std::get<1>(req) >= 1536) {
            requested_from_leecher = true;
            break;
        }
    }
    if (!requested_from_leecher) {
        return fail("scheduler must reserve some requests for chunks already available from non-seed peers");
    }

    requests.clear();

    Scheduler tie_break_sched;
    tie_break_sched.init(
        4,
        1000,
        [&](uint32_t peer_slot, uint32_t chunk_idx, uint32_t begin, uint32_t length) {
            requests.emplace_back(peer_slot, chunk_idx, begin, length);
        },
        [](uint32_t) {},
        []() {});
    tie_break_sched.set_chunk_sizes(std::vector<uint32_t>(4, SUB_BLOCK_SIZE));
    tie_break_sched.mark_all_complete({true, true, true, false});

    tie_break_sched.on_peer_added(1, 1000);
    tie_break_sched.on_bitfield(1, {true, true, true, true});
    tie_break_sched.on_choke_change(1, false);

    tie_break_sched.on_peer_added(2, 1000);
    tie_break_sched.on_bitfield(2, {false, false, false, true});
    tie_break_sched.on_choke_change(2, false);

    tie_break_sched.tick();

    if (requests.empty() || std::get<0>(requests.front()) != 2) {
        return fail("scheduler should prefer a partial peer over the full seed when both can serve the same needed chunk");
    }

    requests.clear();

    Scheduler seed_cap_sched;
    seed_cap_sched.init(
        512,
        1000,
        [&](uint32_t peer_slot, uint32_t chunk_idx, uint32_t begin, uint32_t length) {
            requests.emplace_back(peer_slot, chunk_idx, begin, length);
        },
        [](uint32_t) {},
        []() {});
    seed_cap_sched.set_chunk_sizes(std::vector<uint32_t>(512, SUB_BLOCK_SIZE));
    seed_cap_sched.on_peer_added(1, 1000);
    seed_cap_sched.on_bitfield(1, std::vector<bool>(512, true));
    seed_cap_sched.on_choke_change(1, false);
    std::vector<bool> partial(512, false);
    for (uint32_t i = 0; i < 128; ++i) partial[i] = true;
    seed_cap_sched.on_peer_added(2, 1000);
    seed_cap_sched.on_bitfield(2, partial);
    seed_cap_sched.on_choke_change(2, false);
    seed_cap_sched.tick();

    size_t seed_requests = 0;
    size_t partial_requests = 0;
    for (const auto& req : requests) {
        if (std::get<0>(req) == 1) seed_requests++;
        if (std::get<0>(req) == 2) partial_requests++;
    }
    if (seed_requests > 32 || partial_requests == 0) {
        return fail("full seed should be softly capped when partial peers can serve chunks");
    }

    requests.clear();

    Scheduler cancel_sched;
    size_t cancel_calls = 0;
    cancel_sched.init(
        1,
        1000,
        [&](uint32_t peer_slot, uint32_t chunk_idx, uint32_t begin, uint32_t length) {
            requests.emplace_back(peer_slot, chunk_idx, begin, length);
        },
        [](uint32_t) {},
        []() {});
    cancel_sched.set_cancel_issuer(
        [&](uint32_t, uint32_t, uint32_t, uint32_t) {
            cancel_calls++;
        });
    cancel_sched.set_chunk_sizes({SUB_BLOCK_SIZE * 2});
    cancel_sched.on_peer_added(1, 1000);
    cancel_sched.on_bitfield(1, {true});
    cancel_sched.on_choke_change(1, false);
    cancel_sched.tick();
    if (auto* slot = cancel_sched.peer_slot(1)) {
        if (slot->pending_requests != 2)
            return fail("test setup expected two pending seed requests");
    }
    std::vector<ChunkCompleteMsg> completed{{0, 99}};
    cancel_sched.process_completions(completed);
    if (auto* slot = cancel_sched.peer_slot(1)) {
        if (slot->pending_requests != 0 || !slot->active_sub_blocks.empty()) {
            return fail("cancelling losing requests after chunk completion must release scheduler pending state");
        }
    }
    if (cancel_calls != 2) {
        return fail("chunk completion should cancel each losing sub-block request");
    }

    requests.clear();

    Scheduler rebalance_sched;
    size_t rebalance_cancels = 0;
    rebalance_sched.init(
        4,
        1000,
        [&](uint32_t peer_slot, uint32_t chunk_idx, uint32_t begin, uint32_t length) {
            requests.emplace_back(peer_slot, chunk_idx, begin, length);
        },
        [](uint32_t) {},
        []() {});
    rebalance_sched.set_cancel_issuer(
        [&](uint32_t, uint32_t, uint32_t, uint32_t) {
            rebalance_cancels++;
        });
    rebalance_sched.set_chunk_sizes(std::vector<uint32_t>(4, SUB_BLOCK_SIZE));
    rebalance_sched.on_peer_added(1, 1000);
    rebalance_sched.on_bitfield(1, std::vector<bool>(4, true));
    rebalance_sched.on_choke_change(1, false);
    rebalance_sched.tick();
    requests.clear();
    rebalance_sched.on_peer_added(2, 1000);
    rebalance_sched.on_choke_change(2, false);
    rebalance_sched.on_bitfield(2, {true, false, false, false});

    bool moved_to_partial = false;
    for (const auto& req : requests) {
        if (std::get<0>(req) == 2)
            moved_to_partial = true;
    }
    if (!moved_to_partial || rebalance_cancels == 0) {
        return fail("new partial peer availability should rebalance already-requested seed work");
    }

    requests.clear();

    Scheduler refill_sched;
    refill_sched.init(
        2,
        1000,
        [&](uint32_t peer_slot, uint32_t chunk_idx, uint32_t begin, uint32_t length) {
            requests.emplace_back(peer_slot, chunk_idx, begin, length);
        },
        [](uint32_t) {},
        []() {});
    refill_sched.set_chunk_sizes({SUB_BLOCK_SIZE, SUB_BLOCK_SIZE});

    refill_sched.on_peer_added(7, 1000);
    refill_sched.on_bitfield(7, {true, true});
    refill_sched.on_choke_change(7, false);
    if (auto* slot = refill_sched.peer_slot(7)) {
        slot->pipeline_cap = 1;
    }

    refill_sched.tick();
    if (requests.size() != 1 || std::get<1>(requests.front()) != 0) {
        return fail("initial scheduling should start from the first missing chunk");
    }

    requests.clear();
    refill_sched.dec_peer_pending(7, 0, 0);
    if (requests.size() != 1 || std::get<0>(requests.front()) != 7 || std::get<1>(requests.front()) != 1) {
        return fail("finishing a sub-block should immediately refill the peer pipeline without waiting for the 100ms heartbeat");
    }

    std::cout << "Scheduler tests passed!" << std::endl;
    return 0;
}
