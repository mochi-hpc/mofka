/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <mofka/Client.hpp>
#include <spdlog/spdlog.h>
#include <tclap/CmdLine.h>
#include <iostream>

namespace tl = thallium;

static std::string g_ssgfile;
static std::string g_protocol;
static std::string g_topic;
static std::string g_log_level = "info";

static void parse_command_line(int argc, char** argv);

int main(int argc, char** argv) {
    parse_command_line(argc, argv);
    spdlog::set_level(spdlog::level::from_str(g_log_level));

    // Initialize the thallium server
    tl::engine engine(g_protocol, THALLIUM_CLIENT_MODE);

    try {

        // Initialize a Client
        mofka::Client client(engine);

        // Create ServiceHandle
        mofka::ServiceHandle service = client.connect(mofka::SSGFileName{g_ssgfile});

    } catch(const mofka::Exception& ex) {
        std::cerr << ex.what() << std::endl;
        exit(-1);
    }

    return 0;
}

void parse_command_line(int argc, char** argv) {
    try {
        TCLAP::CmdLine cmd("Mofka client", ' ', "0.1");
        TCLAP::ValueArg<std::string> ssgFileArg(
            "s", "ssgfile", "SSG file of the service", true, "", "string");
        TCLAP::ValueArg<std::string> protocolArg(
            "p", "protocol", "Protocol", true, "na+sm", "string");
        TCLAP::ValueArg<std::string> topicArg(
            "r", "topic", "Topic id", true, mofka::UUID().to_string(), "string");
        TCLAP::ValueArg<std::string> logLevel(
            "v", "verbose", "Log level (trace, debug, info, warning, error, critical, off)", false, "info", "string");
        cmd.add(ssgFileArg);
        cmd.add(protocolArg);
        cmd.add(topicArg);
        cmd.add(logLevel);
        cmd.parse(argc, argv);
        g_ssgfile = ssgFileArg.getValue();
        g_protocol = protocolArg.getValue();
        g_topic = topicArg.getValue();
        g_log_level = logLevel.getValue();
    } catch(TCLAP::ArgException &e) {
        std::cerr << "error: " << e.error() << " for arg " << e.argId() << std::endl;
        exit(-1);
    }
}
