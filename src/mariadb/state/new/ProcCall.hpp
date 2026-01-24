//
// Created by cheesekun on 3/13/23.
//

#ifndef ULTRAVERSE_PROCASSIST_PROCCALL_HPP
#define ULTRAVERSE_PROCASSIST_PROCCALL_HPP

#include <cstdint>

#include <vector>
#include <string>
#include <map>

#include "mariadb/state/new/proto/ultraverse_state_fwd.hpp"

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

  const std::map<std::string, StateData> &args() const;
  const std::map<std::string, StateData> &vars() const;
  void setArgs(const std::map<std::string, StateData> &args);
  void setVars(const std::map<std::string, StateData> &vars);

  std::vector<std::string> &statements();
  
  template <typename Archive>
  void load(Archive &archive);
  
  template <typename Archive>
  void save(Archive &archive) const;

  void toProtobuf(ultraverse::state::v2::proto::ProcCall *out) const;
  void fromProtobuf(const ultraverse::state::v2::proto::ProcCall &msg);
  
  std::vector<StateItem> buildItemSet(const ultraverse::state::v2::ProcMatcher &procMatcher) const;
  std::map<std::string, StateData> buildInitialVariables(const ultraverse::state::v2::ProcMatcher &procMatcher) const;
  
private:
  uint64_t _callId;
  std::string _procName;
  std::string _callInfo;

  std::map<std::string, StateData> _args;
  std::map<std::string, StateData> _vars;

  std::vector<std::string> _statements;
};

#endif // ULTRAVERSE_PROCASSIST_PROCCALL_HPP
