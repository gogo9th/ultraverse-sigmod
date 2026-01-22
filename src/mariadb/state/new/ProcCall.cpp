//
// Created by cheesekun on 3/13/23.
//

#include "ProcCall.hpp"

ProcCall::ProcCall():
  _callId(0),
  _procName(),
  _callInfo(),
  _args(),
  _vars()
{

}
std::string ProcCall::procName() const {
  return _procName;
}

uint64_t ProcCall::callId() const {
  return _callId;
}
void ProcCall::setCallId(uint64_t callId) {
  _callId = callId;
}

std::string ProcCall::callInfo() const {
  return _callInfo;
}
void ProcCall::setCallInfo(const std::string &callInfo) {
  _callInfo = callInfo;
}

const std::map<std::string, StateData> &ProcCall::args() const {
  return _args;
}

const std::map<std::string, StateData> &ProcCall::vars() const {
  return _vars;
}

void ProcCall::setArgs(const std::map<std::string, StateData> &args) {
  _args = args;
}

void ProcCall::setVars(const std::map<std::string, StateData> &vars) {
  _vars = vars;
}

void ProcCall::setProcName(const std::string &procName) {
  _procName = procName;
}
std::vector<std::string> &ProcCall::statements() {
  return _statements;
}

std::vector<StateItem> ProcCall::buildItemSet(const ultraverse::state::v2::ProcMatcher &procMatcher) const {
    std::vector<StateItem> itemSet;

    for (const auto &name : procMatcher.parameters()) {
        auto it = _args.find(name);
        if (it == _args.end()) {
            continue;
        }
        itemSet.emplace_back(StateItem::EQ(name, it->second));
    }
   
    return itemSet;
}

std::map<std::string, StateData> ProcCall::buildInitialVariables(
    const ultraverse::state::v2::ProcMatcher &procMatcher
) const {
    std::map<std::string, StateData> variables;

    for (const auto &name : procMatcher.parameters()) {
        auto it = _args.find(name);
        if (it == _args.end()) {
            continue;
        }
        variables.emplace(name, it->second);
    }

    for (const auto &entry : _vars) {
        variables.emplace(entry.first, entry.second);
    }
    
    return variables;
}
