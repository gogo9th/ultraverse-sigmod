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

#include <cereal/archives/binary.hpp>
#include <cereal/types/map.hpp>
#include <cereal/types/vector.hpp>

#include "Transaction.hpp"

namespace ultraverse::state::v2 {

    struct StateChangeReplayPlan {
        std::vector<gid_t> gids;
        std::map<gid_t, Transaction> userQueries;
        std::vector<gid_t> rollbackGids;
        std::vector<std::string> replaceQueries;

        template <typename Archive>
        void serialize(Archive &archive) {
            archive(gids, userQueries, rollbackGids, replaceQueries);
        }

        void save(const std::string &path) const {
            std::ofstream stream(path, std::ios::binary);
            if (!stream.is_open()) {
                throw std::runtime_error("cannot open replay plan file for write: " + path);
            }
            cereal::BinaryOutputArchive archive(stream);
            archive(*this);
        }

        static StateChangeReplayPlan load(const std::string &path) {
            StateChangeReplayPlan plan;
            std::ifstream stream(path, std::ios::binary);
            if (!stream.is_open()) {
                throw std::runtime_error("cannot open replay plan file for read: " + path);
            }
            cereal::BinaryInputArchive archive(stream);
            archive(plan);
            return plan;
        }
    };
}

#endif // ULTRAVERSE_STATECHANGE_REPLAYPLAN_HPP
