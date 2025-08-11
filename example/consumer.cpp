#include <mofka/MofkaDriver.hpp>
#include <diaspora/Driver.hpp>
#include <diaspora/TopicHandle.hpp>
#include <tclap/CmdLine.h>
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <time.h>
#include <string>
#include <iostream>

static std::string g_group_file;
static std::string g_protocol;
static std::string g_log_level = "info";

static void parse_command_line(int argc, char** argv);

int main(int argc, char** argv) {
    parse_command_line(argc, argv);
    spdlog::set_level(spdlog::level::from_str(g_log_level));

    try {
        // -- Create MofkaDriver
        diaspora::Metadata options;
        options.json()["group_file"] = g_group_file;
        options.json()["margo"] = nlohmann::json::object();
        options.json()["margo"]["use_progress_thread"] = true;

        // -- Create MofkaDriver
        diaspora::Driver driver = diaspora::Driver::New("mofka", options);

        // -- Open a topic
        diaspora::TopicHandle topic = driver.openTopic("mytopic");

        // -- Create a DataSelector for the consumer
        // This example will only select events with an even id field and a value field lower than 70
        // Note: a selector returns a DataDescriptor object that must be built from the DataDescriptor
        // passed as argument. The descriptor argument can be seen as a key to access the underlying data.
        // Returning it tells Mofka "I want all the data from this descriptor". But DataDescriptor has
        // operations that can be used to tell Mofka we are interested in only a subset (e.g. a strided
        // pattern over the data, or a sub-region, like here with makeSubView).
        // diaspora::DataDescriptor::Null() is our way to say we are not interested in this event's data.
        diaspora::DataSelector selector = [](const diaspora::Metadata& metadata,
                                          const diaspora::DataDescriptor& descriptor) {
            if(metadata.json()["id"].get<uint64_t>() % 2 == 0) {
                if(metadata.json()["value"].get<uint64_t>() < 70) {
                    return descriptor; // we want the full data
                } else {
                    return descriptor.makeSubView(2, 4); // we want only 4 bytes from offset 2
                }
            } else {
                return diaspora::DataDescriptor(); // we don't want any data
            }
        };

        // -- Create a DataBroker for the consumer
        diaspora::DataAllocator allocator = [](const diaspora::Metadata& metadata,
                                            const diaspora::DataDescriptor& descriptor) {
            (void)metadata;
            return diaspora::DataView{new char[descriptor.size()], descriptor.size()};
        };

        // -- Get a consumer for the topic
        diaspora::BatchSize   batchSize   = diaspora::BatchSize::Adaptive();
        diaspora::ThreadCount threadCount = diaspora::ThreadCount{1};
        diaspora::Consumer consumer = topic.consumer("myconsumer", batchSize, threadCount, selector, allocator);

        // -- Consume events
        for(size_t i=0; i < 1000; ++i) {
            auto event = consumer.pull().wait();
            if(event.id() == diaspora::NoMoreEvents)
                break;
            auto data = event.data();
            std::string_view data_str{nullptr, 0};
            if(data.size() != 0)
                data_str = std::string_view{
                    reinterpret_cast<const char*>(data.segments()[0].ptr),
                    data.segments()[0].size
                };
            spdlog::info("Received event {} with metadata {} and data {}",
                         event.id(), event.metadata().string(), data_str);
            if(i % 10 == 0) event.acknowledge();
            if(data.size()) delete[] reinterpret_cast<const char*>(data.segments()[0].ptr);
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
        cmd.add(groupFileArg);
        cmd.add(protocolArg);
        cmd.add(logLevel);
        cmd.parse(argc, argv);
        g_group_file = groupFileArg.getValue();
        g_protocol   = protocolArg.getValue();
        g_log_level  = logLevel.getValue();
    } catch(TCLAP::ArgException &e) {
        std::cerr << "error: " << e.error() << " for arg " << e.argId() << std::endl;
        exit(-1);
    }
}
