//
// Created by cheesekun on 3/13/23.
//

#include "ProcCall.hpp"

ProcCall::ProcCall():
  _procName(),
  _callId(0)
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

void ProcCall::setProcName(const std::string &procName) {
  _procName = procName;
}
std::vector<std::string> &ProcCall::statements() {
  return _statements;
}
