#include <cstdio>

#include <iostream>
#include <string>
#include <sstream>
#include <memory>

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

static int procassist_notify(MYSQL_THD thd __attribute__((unused)),
                             mysql_event_class_t event_class,
                              const void *event)
{
  if (plugin == nullptr) {
    return 0;
  }
  
  plugin->handleEvent(thd, event_class, event);
  
  return 0;
}


/*
  Plugin type-specific descriptor
*/
static struct st_mysql_audit procassist_descriptor=
{
  MYSQL_AUDIT_INTERFACE_VERSION, nullptr,
  procassist_notify,
  { MYSQL_AUDIT_GENERAL_CLASS | MYSQL_AUDIT_TABLE_ACCESS_ALL }
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

#ifdef MARIADBD_BASE_VERSION

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


#else

mysql_declare_plugin(ultraverse_procassist)
{
  MYSQL_AUDIT_PLUGIN,
  &procassist_descriptor,
  "ULTRAVERSE_PROCASSIST",
  "the ultraverse authors",
  "",
  PLUGIN_LICENSE_GPL,
  audit_null_plugin_init,
  nullptr,
  audit_null_plugin_deinit,
  0x0002,
  nullptr,  // procassist_status,
  nullptr,
  nullptr,
  0,
}
mysql_declare_plugin_end;

#endif