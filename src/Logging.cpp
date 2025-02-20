/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "Logging.hpp"
#include <margo-logging.h>
#include <spdlog/spdlog.h>

namespace mofka {

static void spdlog_trace(void* uargs, const char* message) {
    (void)uargs;
    spdlog::trace(message);
}

static void spdlog_debug(void* uargs, const char* message) {
    (void)uargs;
    spdlog::debug(message);
}

static void spdlog_info(void* uargs, const char* message) {
    (void)uargs;
    spdlog::info(message);
}

static void spdlog_warning(void* uargs, const char* message) {
    (void)uargs;
    spdlog::warn(message);
}

static void spdlog_error(void* uargs, const char* message) {
    (void)uargs;
    spdlog::error(message);
}

static void spdlog_critical(void* uargs, const char* message) {
    (void)uargs;
    spdlog::critical(message);
}

void setupLogging(margo_instance_id mid) {

    struct margo_logger logger;
    logger.uargs    = NULL;
    logger.trace    = spdlog_trace;
    logger.debug    = spdlog_debug;
    logger.info     = spdlog_info;
    logger.warning  = spdlog_warning;
    logger.error    = spdlog_error;
    logger.critical = spdlog_critical;

    margo_set_logger(mid, &logger);
    switch(spdlog::get_level()) {
        case spdlog::level::trace:    margo_set_log_level(mid, MARGO_LOG_TRACE); break;
        case spdlog::level::debug:    margo_set_log_level(mid, MARGO_LOG_DEBUG); break;
        case spdlog::level::info:     margo_set_log_level(mid, MARGO_LOG_INFO); break;
        case spdlog::level::warn:     margo_set_log_level(mid, MARGO_LOG_WARNING); break;
        case spdlog::level::err:      margo_set_log_level(mid, MARGO_LOG_ERROR); break;
        case spdlog::level::critical: margo_set_log_level(mid, MARGO_LOG_CRITICAL); break;
        default:                      margo_set_log_level(mid, MARGO_LOG_CRITICAL); break;
    }
}

} // namespace bedrock
