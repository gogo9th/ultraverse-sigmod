//
// Created by cheesekun on 3/13/23.
//

#include "ProcCall.hpp"

ProcCall::ProcCall():
  _callId(0),
  _procName(),
  _callInfo()
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

void ProcCall::setProcName(const std::string &procName) {
  _procName = procName;
}
std::vector<std::string> &ProcCall::statements() {
  return _statements;
}

std::vector<StateData> &ProcCall::parameters() {
    return _parameters;
}

void ProcCall::setParameters(const std::vector<StateData> &parameters) {
    _parameters = parameters;
}

std::vector<StateItem> ProcCall::buildItemSet(const ultraverse::state::v2::ProcMatcher &procMatcher) const {
    std::vector<StateItem> itemSet;
    
    if (procMatcher.parameters().size() != _parameters.size()) {
        return itemSet;
    }
    
    for (int i = 0; i < _parameters.size(); i++) {
        const auto &name = procMatcher.parameters()[i];
        const auto &value = _parameters[i];
        
        itemSet.emplace_back(StateItem::EQ(name, value));
    }
   
    return itemSet;
}

std::map<std::string, StateData> ProcCall::buildInitialVariables(
    const ultraverse::state::v2::ProcMatcher &procMatcher
) const {
    std::map<std::string, StateData> variables;
    
    if (procMatcher.parameters().size() != _parameters.size()) {
        return variables;
    }
    
    for (size_t i = 0; i < _parameters.size(); i++) {
        const auto &name = procMatcher.parameters()[i];
        const auto &value = _parameters[i];
        
        variables.emplace(name, value);
    }
    
    return variables;
}
