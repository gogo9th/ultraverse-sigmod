//
// Created by cheesekun on 8/10/22.
//

#include "log.hpp"

#include <map>
#include <mutex>

using LoggerPtr = std::shared_ptr<spdlog::logger>;

inline auto initLoggerSink() {
    auto sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
    sink->set_level(spdlog::level::trace);
    
    return sink;
}

std::shared_ptr<spdlog::sinks::stderr_color_sink_mt> loggerSink = initLoggerSink();
std::map<std::string, LoggerPtr> loggerMap;
std::mutex loggerMutex;

LoggerPtr createLogger(const std::string &name) {
    std::scoped_lock lock(loggerMutex);

    if (loggerMap.find(name) == loggerMap.end()) {
        auto logger = std::make_shared<spdlog::logger>(name, loggerSink);
        
        logger->set_level(loggerSink->level());
        loggerMap.emplace(name, std::move(logger));
    }
    
    return loggerMap[name];
}

void setLogLevel(spdlog::level::level_enum level) {
    std::scoped_lock lock(loggerMutex);
    loggerSink->set_level(level);
    
    for (auto &pair: loggerMap) {
        pair.second->set_level(level);
    }
}

/**
  Auxiliary function used by error() and warning().

  Prints the given text (normally "WARNING: " or "ERROR: "), followed
  by the given vprintf-style string, followed by a newline.

  @param format Printf-style format string.
  @param args List of arguments for the format string.
  @param msg Text to print before the string.
*/
static void error_or_warning(const char *format, va_list args, const char *msg)
{
    fprintf(stderr, "%s: ", msg);
    vfprintf(stderr, format, args);
    fprintf(stderr, "\n");
    fflush(stderr);
}

void debug(const char *format, ...)
{
    va_list args;
    va_start(args, format);
    fprintf(stderr, "DEBUG: ");
    vfprintf(stderr, format, args);
    fprintf(stderr, "\n");
    fflush(stderr);
    va_end(args);
}

/**
  This function is used in log_event.cc to report errors.

  @param format Printf-style format string, followed by printf
  varargs.
*/
static void sql_print_error(const char *format, ...)
{
    va_list args;
    va_start(args, format);
    error_or_warning(format, args, "ERROR");
    va_end(args);
}

/**
  Prints a message to stderr, prefixed with the text "ERROR: " and
  suffixed with a newline.

  @param format Printf-style format string, followed by printf
  varargs.
*/
void error(const char *format, ...)
{
    va_list args;
    va_start(args, format);
    error_or_warning(format, args, "ERROR");
    va_end(args);
}

/**
  Prints a message to stderr, prefixed with the text "WARNING: " and
  suffixed with a newline.

  @param format Printf-style format string, followed by printf
  varargs.
*/
void warning(const char *format, ...)
{
    va_list args;
    va_start(args, format);
    error_or_warning(format, args, "WARNING");
    va_end(args);
}
