//
// Created by cheesekun on 3/13/23.
//

#ifndef ULTRAVERSE_PROCASSIST_PROCCALL_HPP
#define ULTRAVERSE_PROCASSIST_PROCCALL_HPP

#include <cstdint>

#include <vector>
#include <string>

#include "../StateItem.h"
#include "ProcMatcher.hpp"


struct ProcCallHeader {
  uint64_t callId;
  uint64_t nextPos;
} __attribute__ ((packed));

class ProcCall {
public:
  ProcCall();
  
  uint64_t callId() const;
  void setCallId(uint64_t callId);
  
  std::string procName() const;
  void setProcName(const std::string &procName);

  std::string callInfo() const;
  void setCallInfo(const std::string &callInfo);
  
  std::vector<StateData> &parameters();
  void setParameters(const std::vector<StateData> &parameters);

  std::vector<std::string> &statements();
  
  template <typename Archive>
  void load(Archive &archive);
  
  template <typename Archive>
  void save(Archive &archive) const;
  
  std::vector<StateItem> buildItemSet(const ultraverse::state::v2::ProcMatcher &procMatcher) const;
  
private:
  uint64_t _callId;
  std::string _procName;
  std::string _callInfo;
  
  std::vector<StateData> _parameters;
  

  std::vector<std::string> _statements;
};

#include "ProcCall.cereal.cpp"

#endif // ULTRAVERSE_PROCASSIST_PROCCALL_HPP
