#include <mofka/MofkaDriver.hpp>
#include <diaspora/Driver.hpp>
#include <diaspora/TopicHandle.hpp>
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <cstdlib>
#include <ctime>
#include <string>
#include <iostream>

int main(int argc, char** argv) {
    if(argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <group-file>" << std::endl;
        return -1;
    }
    std::string g_group_file = argv[1];
    spdlog::set_level(spdlog::level::info);

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
