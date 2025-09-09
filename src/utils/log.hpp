//
// Created by cheesekun on 8/10/22.
//

#ifndef ULTRAVERSE_LOG_HPP
#define ULTRAVERSE_LOG_HPP

#include <cstdio>
#include <cstdarg>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/null_sink.h>

using LoggerPtr = std::shared_ptr<spdlog::logger>;

LoggerPtr createLogger(const std::string &name);

void setLogLevel(spdlog::level::level_enum level);



/**
  Auxiliary function used by error() and warning().

  Prints the given text (normally "WARNING: " or "ERROR: "), followed
  by the given vprintf-style string, followed by a newline.

  @param format Printf-style format string.
  @param args List of arguments for the format string.
  @param msg Text to print before the string.
*/
static void error_or_warning(const char *format, va_list args, const char *msg);
void debug(const char *format, ...);

/**
  This function is used in log_event.cc to report errors.

  @param format Printf-style format string, followed by printf
  varargs.
*/
static void sql_print_error(const char *format, ...);

/**
  Prints a message to stderr, prefixed with the text "ERROR: " and
  suffixed with a newline.

  @param format Printf-style format string, followed by printf
  varargs.
*/
void error(const char *format, ...);

/**
  Prints a message to stderr, prefixed with the text "WARNING: " and
  suffixed with a newline.

  @param format Printf-style format string, followed by printf
  varargs.
*/
void warning(const char *format, ...);


#endif //ULTRAVERSE_LOG_HPP
