//
// Created by cheesekun on 3/13/23.
//

#include <cereal/types/memory.hpp>
#include <cereal/types/utility.hpp>
#include <cereal/types/map.hpp>

#include "ProcCall.hpp"

template <typename Archive>
void ProcCall::save(Archive &archive) const {
  archive(_callId);
  archive(_procName);
  archive(_callInfo);
  archive(_args);
  archive(_vars);
  
  archive(_statements.size());
  for (const auto &statement: _statements) {
    archive(statement);
  }
}

template <typename Archive>
void ProcCall::load(Archive &archive) {
  archive(_callId);
  archive(_procName);
  archive(_callInfo);
  archive(_args);
  archive(_vars);

  size_t size = 0;
  archive(size);
  
  for (int i = 0; i < size; i++) {
    std::string statement;
    archive(statement);
    
    _statements.emplace_back(std::move(statement));
  }
}
