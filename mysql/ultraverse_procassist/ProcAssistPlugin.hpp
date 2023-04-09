//
// Created by cheesekun on 3/13/23.
//

#ifndef MYSQL_PROCASSISTPLUGIN_HPP
#define MYSQL_PROCASSISTPLUGIN_HPP

#include <map>
#include <memory>
#include <utility>
#include <fstream>

#include <mysql/plugin.h>
#include <mysql/plugin_audit.h>

#include "ProcCall.hpp"

class ProcAssistPlugin {
public:
  ProcAssistPlugin();
  ~ProcAssistPlugin();
  
  void handleEvent(MYSQL_THD thd, unsigned int eventClass, const void *event);
  
private:
  void handleGeneralEvent(MYSQL_THD thd, const mysql_event_general *event);
  
  void writeProcLog(std::shared_ptr<ProcCall> procCall);
  
  bool isProcedureCall(const std::string &statement);
  bool isProcedureHint(const std::string &statement);
  
  std::tuple<uint64_t, std::string, std::string> extractProcedureHint(const std::string &statement);
  
  /**
   * connectionId, ProcCall
   */
  std::map<long, std::shared_ptr<ProcCall>> _callList;
  
  std::ofstream _procLogStream;
};

#endif // MYSQL_PROCASSISTPLUGIN_HPP
