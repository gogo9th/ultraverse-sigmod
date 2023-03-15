#include <cstdio>

#include <iostream>
#include <string>
#include <sstream>
#include <memory>

#include <my_global.h>

#include <handler.h>

#include <mysql/plugin.h>
#include <mysql/plugin_audit.h>

#include "ProcAssistPlugin.hpp"

static std::shared_ptr<ProcAssistPlugin> plugin;


static volatile int ncalls; /* for SHOW STATUS, see below */
static volatile int ncalls_general_log;
static volatile int ncalls_general_error;
static volatile int ncalls_general_result;

static int audit_null_plugin_init(void *arg __attribute__((unused)))
{
  if (plugin != nullptr) {
    return 1;
  }
  
  plugin = std::make_shared<ProcAssistPlugin>();

  return 0;
}


static int audit_null_plugin_deinit(void *arg __attribute__((unused)))
{
  plugin = nullptr;
  
  return 0;
}

static void procassist_notify(MYSQL_THD thd __attribute__((unused)),
                              unsigned int event_class,
                              const void *event)
{
  if (plugin == nullptr) {
    return;
  }
  
  plugin->handleEvent(thd, event_class, event);
  
  /* prone to races, oh well */
  ncalls++;
  MYSQL_XID xid;
  thd_get_xid(thd, &xid);
  
  if (event_class == MYSQL_AUDIT_GENERAL_CLASS)
  {
  }
  else
  if (event_class == MYSQL_AUDIT_TABLE_CLASS)
  {
    const struct mysql_event_table *event_table=
      (const struct mysql_event_table *) event;
    const char *ip= event_table->ip ? event_table->ip : "";
    const char *op= 0;
    char buf[1024];

    switch (event_table->event_subclass)
    {
    case MYSQL_AUDIT_TABLE_LOCK:
      op= event_table->read_only ? "read" : "write";
      break;
    case MYSQL_AUDIT_TABLE_CREATE:
      op= "create";
      break;
    case MYSQL_AUDIT_TABLE_DROP:
      op= "drop";
      break;
    case MYSQL_AUDIT_TABLE_ALTER:
      op= "alter";
      break;
    case MYSQL_AUDIT_TABLE_RENAME:
      snprintf(buf, sizeof(buf), "rename to %s.%s",
               event_table->new_database.str, event_table->new_table.str);
      buf[sizeof(buf)-1]= 0;
      op= buf;
      break;
    }

    fprintf(stderr, "%s[%s] @ %s [%s]\t%s.%s : %s\n",
            event_table->priv_user, event_table->user,
            event_table->host, ip,
            event_table->database.str, event_table->table.str, op);
  }
  
}


/*
  Plugin type-specific descriptor
*/
static struct st_mysql_audit procassist_descriptor=
{
  MYSQL_AUDIT_INTERFACE_VERSION, nullptr,
  procassist_notify,
  { MYSQL_AUDIT_GENERAL_CLASSMASK | MYSQL_AUDIT_TABLE_CLASSMASK }
};

/*
  Plugin status variables for SHOW STATUS
*/

/*
static struct st_mysql_show_var procassist_status[]=
{
  { "called", (char *) &ncalls, SHOW_INT },
  { "general_error", (char *) &ncalls_general_error, SHOW_INT },
  { "general_log", (char *) &ncalls_general_log, SHOW_INT },
  { "general_result", (char *) &ncalls_general_result, SHOW_INT },
  { 0, 0, SHOW_UNDEF }
};
*/

/*
  Plugin library descriptor
*/

maria_declare_plugin(ultraverse_procassist)
{
  MYSQL_AUDIT_PLUGIN,
  &procassist_descriptor,
  "ULTRAVERSE_PROCASSIST",
  "the ultraverse authors",
  "Assistance Plugin for analyzing SQL procedure calls",
  PLUGIN_LICENSE_PROPRIETARY,
  audit_null_plugin_init,
  audit_null_plugin_deinit,
  0x0002,
  nullptr,  // procassist_status,
  nullptr,
  "0.0.2",
  MariaDB_PLUGIN_MATURITY_BETA
}
maria_declare_plugin_end;

