#include <mofka/MofkaDriver.hpp>
#include <diaspora/Driver.hpp>
#include <diaspora/TopicHandle.hpp>
#include <tclap/CmdLine.h>
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <time.h>
#include <string>
#include <iostream>

static std::string g_backend_type = "memory";
static std::string g_group_file;
static std::string g_protocol;
static std::string g_log_level = "info";

static void parse_command_line(int argc, char** argv);

int main(int argc, char** argv) {
    parse_command_line(argc, argv);
    spdlog::set_level(spdlog::level::from_str(g_log_level));

    try {

        diaspora::Metadata options;
        options.json()["group_file"] = g_group_file;
        options.json()["margo"] = nlohmann::json::object();
        options.json()["margo"]["use_progress_thread"] = true;

        // -- Create MofkaDriver
        diaspora::Driver driver = diaspora::Driver::New("mofka", options);

        // -- Create a topic
        // We provide a default validator, selector, and serializer as example for the API.
        diaspora::Validator         validator;
        diaspora::Serializer        serializer;
        diaspora::PartitionSelector selector;
        driver.createTopic("mytopic", diaspora::Metadata{}, validator, selector, serializer);

        driver.as<mofka::MofkaDriver>().addLegacyPartition("mytopic", 0);

        diaspora::TopicHandle topic = driver.openTopic("mytopic");

        // -- Get a producer for the topic
        diaspora::BatchSize   batchSize   = diaspora::BatchSize::Adaptive();
        diaspora::ThreadCount threadCount = diaspora::ThreadCount{1};
        diaspora::Ordering    ordering    = diaspora::Ordering::Strict;
        diaspora::Producer    producer    = topic.producer("myproducer", batchSize, threadCount, ordering);

        srand(time(nullptr));

        // -- Initialize some random data to be sent
        std::vector<char> buffer(8000);
        for(auto& c : buffer) c = 'A' + (rand() % 26);

        // -- Produce events
        for(size_t i=0; i < 1000; ++i) {
            auto j = rand() % 100;
            diaspora::Metadata metadata = fmt::format("{{\"id\": {}, \"value\": {}}}", i, j);
            diaspora::DataView data{buffer.data() + i*8, 8};
            spdlog::info("Sending event {} with metadata {} and data {}",
                         i, metadata.string(), std::string_view{buffer.data() + i*8, 8});
            auto future = producer.push(metadata, data);
            // The future can be waited on using future.wait().
            // Here we simply drop it and flush the producer ever 100 events
            if(i % 100 == 0) producer.flush();
        }

    } catch(const diaspora::Exception& ex) {
        spdlog::critical("{}", ex.what());
        exit(-1);
    }

    spdlog::info("Done!");

    return 0;
}

static void parse_command_line(int argc, char** argv) {
    try {
        TCLAP::CmdLine cmd("Mofka client", ' ', "0.1");
        TCLAP::ValueArg<std::string> groupFileArg(
                "f", "group-file", "Flock group file of the driver", true, "", "string");
        TCLAP::ValueArg<std::string> protocolArg(
                "p", "protocol", "Protocol", true, "na+sm", "string");
        TCLAP::ValuesConstraint<std::string> allowedLogLevels({
            "trace", "debug", "info", "warning", "error", "critical", "off"
        });
        TCLAP::ValueArg<std::string> logLevel(
                "v", "verbose", "Logging level",
                false, "info", &allowedLogLevels);
        TCLAP::ValuesConstraint<std::string> allowedBackends({
            "memory", "default"
        });
        TCLAP::ValueArg<std::string> backendType(
                "b", "backend", "Backend",
                false, "memory", &allowedBackends);
        cmd.add(groupFileArg);
        cmd.add(protocolArg);
        cmd.add(logLevel);
        cmd.add(backendType);
        cmd.parse(argc, argv);
        g_group_file   = groupFileArg.getValue();
        g_protocol     = protocolArg.getValue();
        g_log_level    = logLevel.getValue();
        g_backend_type = backendType.getValue();
    } catch(TCLAP::ArgException &e) {
        std::cerr << "error: " << e.error() << " for arg " << e.argId() << std::endl;
        exit(-1);
    }
}
