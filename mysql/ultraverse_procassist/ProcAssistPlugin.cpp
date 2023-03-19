//
// Created by cheesekun on 3/13/23.
//

#include <cstdio>
#include <sstream>

#include <my_global.h>

#include <handler.h>

#include <mysql/plugin.h>
#include <mysql/plugin_audit.h>

#include <cereal/archives/binary.hpp>

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

void ProcAssistPlugin::handleEvent(THD *thd, unsigned int eventClass,
                                   const void *event) {
  if (eventClass == MYSQL_AUDIT_GENERAL_CLASS) {
    handleGeneralEvent(thd, reinterpret_cast<const mysql_event_general *>(event));
  }
}
void ProcAssistPlugin::handleGeneralEvent(THD *thd,
                                          const mysql_event_general *event) {
  auto commandId = thd_sql_command(thd);
  long connectionId = event->general_thread_id;
  
  std::string statement(event->general_query, event->general_query_length);
  
  fprintf(stderr, "COMAMND: %s\n", event->general_command);
  /*
  if (event->event_subclass != MYSQL_AUDIT_GENERAL_RESULT) {
    return;
  }
   */
  
  if (event->event_subclass == MYSQL_AUDIT_GENERAL_LOG) {
    fprintf(stderr, "[MYSQL_AUDIT_GENERAL_LOG][%d][%ld] %s\t>> %s\n",
            commandId,
            event->general_thread_id,
            event->general_user,
            event->general_query);
  } else if (event->event_subclass == MYSQL_AUDIT_GENERAL_ERROR) {
     fprintf(stderr, "[MYSQL_AUDIT_GENERAL_ERROR][%d][%ld] %s\t>> %s\n",
            commandId,
            event->general_thread_id,
            event->general_user,
            event->general_query);
  } else if (event->event_subclass == MYSQL_AUDIT_GENERAL_RESULT) {
      fprintf(stderr, "[MYSQL_AUDIT_GENERAL_RESULT][%d][%ld] %s\t>> %s\n",
            commandId,
            event->general_thread_id,
            event->general_user,
            event->general_query);
      
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
          auto pair= extractProcedureHint(statement);

          if (procCall->callId() == 0) {
            fprintf(stderr, "procName: %s / callId: %ld\n", pair.first.c_str(), pair.second);
            procCall->setProcName(pair.first);
            procCall->setCallId(pair.second);
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

std::pair<std::string, uint64_t>
ProcAssistPlugin::extractProcedureHint(const std::string &statement)
{
  std::string procName = "unknown";
  uint64_t callId = 0;
  
  {
    int pos= statement.find('\'') + 1;
    std::stringstream sstream;

    while (statement[pos] != '\'')
    {
      sstream.put(statement[pos]);
      pos++;
    }
    
    procName = sstream.str();
  }

  {
    int pos= statement.rfind(',') + 1;
    std::stringstream sstream;

    while (statement[pos] >= '0' && statement[pos] <= '9')
    {
      sstream.put(statement[pos]);
      pos++;
    }
    
    callId = std::stoul(sstream.str());
  }
  
  return std::make_pair(procName, callId);
}
