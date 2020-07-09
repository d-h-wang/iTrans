/*
 * debuglog.h
 *
 *  Created on: May 5, 2014
 *      Author: tzhu
 */
#include "log.h" //overwrite the following definition
#ifndef DEBUGLOG_H_
#define DEBUGLOG_H_

#include <string>
#include <ctime>
#include <cstdio>

// Log levels.
#define LOG_LEVEL_OFF    1000
#define LOG_LEVEL_ERROR  500
#define LOG_LEVEL_WARN   400
#define LOG_LEVEL_INFO   300
#define LOG_LEVEL_DEBUG  200
#define LOG_LEVEL_TRACE  100
#define LOG_LEVEL_ALL    0

#define LOG_LOG_TIME_FORMAT "%Y-%m-%d %H:%M:%S"

// Compile Option
#ifndef LOG_LEVEL
    #if defined(DEBUG) 
        #define LOG_LEVEL LOG_LEVEL_DEBUG
    #else
        #define LOG_LEVEL LOG_LEVEL_WARN
    #endif
#endif

void outputLogHeader_(const char *file, int line, const char *func, int level);

#ifdef LOG_ERROR_ENABLED
    #undef LOG_ERROR_ENABLED
#endif
#if LOG_LEVEL<=LOG_LEVEL_ERROR
    #define LOG_ERROR_ENABLED
    //#pragma message("LOG_ERROR was enabled.")
    #define LOG_ERROR(...) outputLogHeader_(__FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_ERROR);::printf(__VA_ARGS__);printf("\n");::fflush(stdout)
#else
    #define LOG_ERROR(...) ((void)0)
#endif

#ifdef LOG_WARN_ENABLED
    #undef LOG_WARN_ENABLED
#endif
#if LOG_LEVEL<=LOG_LEVEL_WARN
    #define LOG_WARN_ENABLED
    //#pragma message("LOG_WARN was enabled.")
    #define LOG_WARN(...) outputLogHeader_(__FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_WARN);::printf(__VA_ARGS__);printf("\n");::fflush(stdout)
#else
    #define LOG_WARN(...) ((void)0)
#endif

#ifdef LOG_INFO_ENABLED
    #undef LOG_INFO_ENABLED
#endif
#if LOG_LEVEL<=LOG_LEVEL_INFO
    #define LOG_INFO_ENABLED
    //#pragma message("LOG_INFO was enabled.")
    #define LOG_INFO(...) outputLogHeader_(__FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_INFO);::printf(__VA_ARGS__);printf("\n");::fflush(stdout)
#else
    #define LOG_INFO(...) ((void)0)
#endif

#ifdef LOG_DEBUG_ENABLED
    #undef LOG_DEBUG_ENABLED
#endif
#if LOG_LEVEL<=LOG_LEVEL_DEBUG
    #define LOG_DEBUG_ENABLED
    //#pragma message("LOG_DEBUG was enabled.")
    #define LOG_DEBUG(...) outputLogHeader_(__FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_DEBUG);::printf(__VA_ARGS__);printf("\n");::fflush(stdout)
#else
    #define LOG_DEBUG(...) ((void)0)
#endif

#ifdef LOG_TRACE_ENABLED
    #undef LOG_TRACE_ENABLED
#endif
#if LOG_LEVEL<=LOG_LEVEL_TRACE
    #define LOG_TRACE_ENABLED
    //#pragma message("LOG_TRACE was enabled.")
    #define LOG_TRACE(...) outputLogHeader_(__FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_TRACE);::printf(__VA_ARGS__);printf("\n");::fflush(stdout)
#else
    #define LOG_TRACE(...) ((void)0)
#endif


// Output log message header in this format: [type] [file:line:function] time -
// ex: [ERROR] [somefile.cpp:123:doSome()] 2008/07/06 10:00:00 -
inline void outputLogHeader_(const char *file, int line, const char *func, int level) {
    time_t t = ::time(NULL) ;
    tm *curTime = localtime(&t);
    char time_str[32]; // FIXME
    ::strftime(time_str, 32, LOG_LOG_TIME_FORMAT, curTime);
    const char* type;
    switch (level) {
        case LOG_LEVEL_ERROR:
            type = "ERROR";
            break;
        case LOG_LEVEL_WARN:
            type = "WARN ";
            break;
        case LOG_LEVEL_INFO:
            type = "INFO ";
            break;
        case LOG_LEVEL_DEBUG:
            type = "DEBUG";
            break;
        case LOG_LEVEL_TRACE:
            type = "TRACE";
            break;
        default:
            type = "UNKWN";
    }
    printf("[%s] [%s:%d:%s()] %s - ", type, file, line, func, time_str);
}

#endif /* DEBUGLOG_H_ */
