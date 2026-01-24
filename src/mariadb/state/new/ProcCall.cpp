//
// Created by cheesekun on 3/13/23.
//

#include "ProcCall.hpp"

#include "ultraverse_state.pb.h"

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
        const auto direction = procMatcher.parameterDirection(name);
        if (direction == ultraverse::state::v2::ProcMatcher::ParamDirection::OUT) {
            continue;
        }
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
        const auto direction = procMatcher.parameterDirection(name);
        if (direction == ultraverse::state::v2::ProcMatcher::ParamDirection::OUT) {
            continue;
        }
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

void ProcCall::toProtobuf(ultraverse::state::v2::proto::ProcCall *out) const {
  if (out == nullptr) {
    return;
  }

  out->Clear();
  out->set_call_id(_callId);
  out->set_proc_name(_procName);
  out->set_call_info(_callInfo);

  auto *argsMap = out->mutable_args();
  argsMap->clear();
  for (const auto &pair : _args) {
    auto &dataMsg = (*argsMap)[pair.first];
    pair.second.toProtobuf(&dataMsg);
  }

  auto *varsMap = out->mutable_vars();
  varsMap->clear();
  for (const auto &pair : _vars) {
    auto &dataMsg = (*varsMap)[pair.first];
    pair.second.toProtobuf(&dataMsg);
  }

  for (const auto &statement : _statements) {
    out->add_statements(statement);
  }
}

void ProcCall::fromProtobuf(const ultraverse::state::v2::proto::ProcCall &msg) {
  _callId = msg.call_id();
  _procName = msg.proc_name();
  _callInfo = msg.call_info();

  _args.clear();
  for (const auto &pair : msg.args()) {
    StateData data;
    data.fromProtobuf(pair.second);
    _args.emplace(pair.first, std::move(data));
  }

  _vars.clear();
  for (const auto &pair : msg.vars()) {
    StateData data;
    data.fromProtobuf(pair.second);
    _vars.emplace(pair.first, std::move(data));
  }

  _statements.clear();
  _statements.reserve(static_cast<size_t>(msg.statements_size()));
  for (const auto &statement : msg.statements()) {
    _statements.emplace_back(statement);
  }
}
