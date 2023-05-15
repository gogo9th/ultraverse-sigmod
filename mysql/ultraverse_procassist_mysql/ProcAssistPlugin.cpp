//
// Created by cheesekun on 3/13/23.
//

#include <cstdio>
#include <sstream>

#include <mysql/plugin.h>
#include <mysql/plugin_audit.h>

#include <cereal/archives/binary.hpp>
#include <nlohmann/json.hpp>

#include "ProcAssistPlugin.hpp"

ProcAssistPlugin::ProcAssistPlugin()
{
  std::stringstream logName;
  logName << "proc_log_" << time(nullptr) << ".ultproclog";
  
  _procLogStream = std::ofstream(logName.str(), std::ios::out | std::ios::binary);
}

ProcAssistPlugin::~ProcAssistPlugin() {
  _procLogStream.close();
}

void ProcAssistPlugin::handleEvent(THD *thd, mysql_event_class_t eventClass,
                                   const void *event) {
  if (eventClass == MYSQL_AUDIT_GENERAL_CLASS) {
    handleGeneralEvent(thd, reinterpret_cast<const mysql_event_general *>(event));
  }
}
void ProcAssistPlugin::handleGeneralEvent(THD *thd,
                                          const mysql_event_general *event) {
  auto commandId = thd_sql_command(thd);
  long connectionId = event->general_thread_id;
  
  std::string statement(event->general_query.str, event->general_query.length);
  
  fprintf(stderr, "COMAMND: %s\n", event->general_command.str);
  /*
  if (event->event_subclass != MYSQL_AUDIT_GENERAL_RESULT) {
    return;
  }
   */
  
  if (event->event_subclass == MYSQL_AUDIT_GENERAL_LOG) {
    fprintf(stderr, "[MYSQL_AUDIT_GENERAL_LOG][%d][%ld] %s\t>> %s\n",
            commandId,
            event->general_thread_id,
            event->general_user.str,
            event->general_query.str);
  } else if (event->event_subclass == MYSQL_AUDIT_GENERAL_ERROR) {
     fprintf(stderr, "[MYSQL_AUDIT_GENERAL_ERROR][%d][%ld] %s\t>> %s\n",
            commandId,
            event->general_thread_id,
            event->general_user.str,
            event->general_query.str);
  } else if (event->event_subclass == MYSQL_AUDIT_GENERAL_RESULT) {
      fprintf(stderr, "[MYSQL_AUDIT_GENERAL_RESULT][%d][%ld] %s\t>> %s\n",
            commandId,
            event->general_thread_id,
            event->general_user.str,
            event->general_query.str);
      
  }
  
  if (commandId == SQLCOM_END && isProcedureCall(statement)) {
      if (event->event_subclass == MYSQL_AUDIT_GENERAL_RESULT) {
        writeProcLog(_callList[connectionId]);
      }
      
      if (event->event_subclass == MYSQL_AUDIT_GENERAL_RESULT ||
          event->event_subclass == MYSQL_AUDIT_GENERAL_ERROR) {
        auto procCall= _callList[connectionId];
        if (procCall == nullptr) {
          return;
        }

        fprintf(stderr, "procCall finalized\n");
        fprintf(stderr, "%ld -- %s\n", procCall->callId(),
                procCall->procName().c_str());
        for (const auto &st : procCall->statements())
        {
          fprintf(stderr, "    %s\n", st.c_str());
        }

        _callList[connectionId]= nullptr;
      }
   
      return;
  }
  
  if (event->event_subclass == MYSQL_AUDIT_GENERAL_LOG)
  {
      if (isProcedureCall(statement))
      {
        if (_callList[connectionId] != nullptr) {
          return;
        }

        auto procCall = std::make_shared<ProcCall>();
        procCall->statements().push_back(statement);
        
        _callList[connectionId] = procCall;
      }
      else if (_callList[connectionId] != nullptr)
      {
        if (isProcedureHint(statement))
        {
          auto procCall= _callList[connectionId];
          auto tuple= extractProcedureHint(statement);

          if (procCall->callId() == 0) {
            uint64_t callId = std::get<0>(tuple);
            std::string procName = std::get<1>(tuple);
            std::string callInfo = std::get<2>(tuple);

            fprintf(stderr, "procName: %s / callId: %ld\n", procName.c_str(), callId);
            procCall->setProcName(procName);
            procCall->setCallId(callId);
            procCall->setCallInfo(callInfo);
          }
        }
        else
        {
          _callList[connectionId]->statements().push_back(statement);
        }
      }
  }
  
}

void ProcAssistPlugin::writeProcLog(std::shared_ptr<ProcCall> procCall) {
  if (procCall == nullptr) {
      return;
  }

  std::stringstream sstream;
  cereal::BinaryOutputArchive archive(sstream);
  archive(*procCall);
  
  std::string procCallStr = sstream.str();
  
  ProcCallHeader header {
      procCall->callId(),
      sizeof(ProcCallHeader) + procCallStr.size() + _procLogStream.tellp()
  };
  
  _procLogStream.write((char *) &header, sizeof(header));
  _procLogStream.write(procCallStr.c_str(), procCallStr.size());
  _procLogStream.flush();
}

bool ProcAssistPlugin::isProcedureCall(const std::string &statement)
{
  return statement.find("CALL ") == 0;
}

bool ProcAssistPlugin::isProcedureHint(const std::string &statement)
{
  return statement.find("INSERT INTO __ULTRAVERSE_PROCEDURE_HINT") == 0;
}

std::tuple<uint64_t, std::string, std::string>
ProcAssistPlugin::extractProcedureHint(const std::string &statement)
{
  using namespace nlohmann;

  std::string procName = "unknown";
  uint64_t callId = 0;

  int nameConstPos = statement.find("_utf8mb4");
  int nameConstRPos = statement.rfind("COLLATE");
  int lpos = statement.find('\'', nameConstPos) + 1;
  int rpos = statement.rfind('\'', nameConstRPos);

  std::string jsonStr = statement.substr(lpos, rpos - lpos);
  // FIXME
  jsonStr.erase(std::remove(jsonStr.begin(), jsonStr.end(), '\\'), jsonStr.end());

  fprintf(stderr, "JSON: %s\n", jsonStr.c_str());

  auto jsonObj = json::parse(jsonStr);

  callId = jsonObj.at(0).get<uint64_t>();
  procName = jsonObj.at(1).get<std::string>();

  return std::make_tuple(callId, procName, jsonStr);
}
