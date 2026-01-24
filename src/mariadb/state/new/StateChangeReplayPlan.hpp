//
// Created by cheesekun on 1/20/26.
//

#ifndef ULTRAVERSE_STATECHANGE_REPLAYPLAN_HPP
#define ULTRAVERSE_STATECHANGE_REPLAYPLAN_HPP

#include <fstream>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "ultraverse_state.pb.h"

#include "Transaction.hpp"

namespace ultraverse::state::v2 {

    struct StateChangeReplayPlan {
        std::vector<gid_t> gids;
        std::map<gid_t, Transaction> userQueries;
        std::vector<gid_t> rollbackGids;
        std::vector<std::string> replaceQueries;

        void toProtobuf(ultraverse::state::v2::proto::StateChangeReplayPlan *out) const {
            if (out == nullptr) {
                return;
            }

            out->Clear();
            for (const auto gid : gids) {
                out->add_gids(gid);
            }

            auto *userQueryMap = out->mutable_user_queries();
            userQueryMap->clear();
            for (const auto &pair : userQueries) {
                auto &txnMsg = (*userQueryMap)[pair.first];
                pair.second.toProtobuf(&txnMsg);
            }

            for (const auto gid : rollbackGids) {
                out->add_rollback_gids(gid);
            }

            for (const auto &query : replaceQueries) {
                out->add_replace_queries(query);
            }
        }

        void fromProtobuf(const ultraverse::state::v2::proto::StateChangeReplayPlan &msg) {
            gids.clear();
            gids.reserve(static_cast<size_t>(msg.gids_size()));
            for (const auto gid : msg.gids()) {
                gids.push_back(gid);
            }

            userQueries.clear();
            for (const auto &pair : msg.user_queries()) {
                Transaction txn;
                txn.fromProtobuf(pair.second);
                userQueries.emplace(pair.first, std::move(txn));
            }

            rollbackGids.clear();
            rollbackGids.reserve(static_cast<size_t>(msg.rollback_gids_size()));
            for (const auto gid : msg.rollback_gids()) {
                rollbackGids.push_back(gid);
            }

            replaceQueries.clear();
            replaceQueries.reserve(static_cast<size_t>(msg.replace_queries_size()));
            for (const auto &query : msg.replace_queries()) {
                replaceQueries.emplace_back(query);
            }
        }

        void save(const std::string &path) const {
            std::ofstream stream(path, std::ios::binary);
            if (!stream.is_open()) {
                throw std::runtime_error("cannot open replay plan file for write: " + path);
            }
            ultraverse::state::v2::proto::StateChangeReplayPlan protoPlan;
            toProtobuf(&protoPlan);
            if (!protoPlan.SerializeToOstream(&stream)) {
                throw std::runtime_error("failed to serialize replay plan protobuf: " + path);
            }
        }

        static StateChangeReplayPlan load(const std::string &path) {
            StateChangeReplayPlan plan;
            std::ifstream stream(path, std::ios::binary);
            if (!stream.is_open()) {
                throw std::runtime_error("cannot open replay plan file for read: " + path);
            }
            ultraverse::state::v2::proto::StateChangeReplayPlan protoPlan;
            if (!protoPlan.ParseFromIstream(&stream)) {
                throw std::runtime_error("failed to read replay plan protobuf: " + path);
            }
            plan.fromProtobuf(protoPlan);
            return plan;
        }
    };
}

#endif // ULTRAVERSE_STATECHANGE_REPLAYPLAN_HPP
