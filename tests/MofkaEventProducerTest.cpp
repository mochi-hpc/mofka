/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <bedrock/Server.hpp>
#include <diaspora/Driver.hpp>
#include <diaspora/TopicHandle.hpp>
#include "Configs.hpp"
#include "Ensure.hpp"

TEST_CASE("Event producer test", "[event-producer]") {

    spdlog::set_level(spdlog::level::from_str("critical"));
    auto partition_type = GENERATE(as<std::string>{}, "memory", "default");
    CAPTURE(partition_type);
    auto remove_file = EnsureFileRemoved{"mofka.json"};

    auto server = bedrock::Server("na+sm", config);
    ENSURE(server.finalize());
    auto engine = server.getMargoManager().getThalliumEngine();

    SECTION("Initialize client/topic/producer") {
        diaspora::Metadata options;
        options.json()["group_file"] = "mofka.json";
        options.json()["margo"] = nlohmann::json::object();
        options.json()["margo"]["use_progress_thread"] = true;
        diaspora::Driver driver = diaspora::Driver::New("mofka", options);
        REQUIRE(static_cast<bool>(driver));
        diaspora::TopicHandle topic;
        REQUIRE(!static_cast<bool>(topic));
        REQUIRE_NOTHROW(driver.createTopic("mytopic"));
        diaspora::Metadata partition_config;
        mofka::MofkaDriver::Dependencies partition_dependencies;
        getPartitionArguments(partition_type, partition_dependencies, partition_config);

        REQUIRE_NOTHROW(driver.as<mofka::MofkaDriver>().addCustomPartition(
                    "mytopic", 0, partition_type,
                    partition_config, partition_dependencies));
        REQUIRE_NOTHROW(topic = driver.openTopic("mytopic"));
        REQUIRE(static_cast<bool>(topic));

        auto thread_count = GENERATE(as<diaspora::ThreadCount>{}, 0, 1, 2);
        auto batch_size   = GENERATE(diaspora::BatchSize::Adaptive(), 10);
        auto ordering     = GENERATE(diaspora::Ordering::Strict, diaspora::Ordering::Loose);

        auto producer = topic.producer(
            "myproducer", batch_size, thread_count, ordering);
        REQUIRE(static_cast<bool>(producer));

        {
            for(size_t i = 0; i < 100; ++i) {
                diaspora::Future<diaspora::EventID> future;
                auto metadata = diaspora::Metadata{
                    fmt::format("{{\"event_num\":{}}}", i)
                };
                REQUIRE_NOTHROW(future = producer.push(metadata, diaspora::DataView{0, nullptr}));
                if((i+1) % 5 == 0) {
                    if(batch_size != diaspora::BatchSize::Adaptive())
                        REQUIRE_NOTHROW(producer.flush());
                    REQUIRE_NOTHROW(future.wait());
                }
            }
            REQUIRE_NOTHROW(producer.flush());
        }

        std::vector<std::string> data(100);
        {
            for(size_t i = 0; i < 100; ++i) {
                diaspora::Future<diaspora::EventID> future;
                auto metadata = diaspora::Metadata{
                    fmt::format("{{\"event_num\":{}}}", i)
                };
                data[i] = fmt::format("This is some data for event {}", i);
                REQUIRE_NOTHROW(future = producer.push(
                            metadata,
                            diaspora::DataView{data[i].data(), data[i].size()}));
                if((i+1) % 5 == 0) {
                    if(batch_size != diaspora::BatchSize::Adaptive())
                        REQUIRE_NOTHROW(producer.flush());
                    REQUIRE_NOTHROW(future.wait());
                }
            }
            REQUIRE_NOTHROW(producer.flush());
        }
    }
}
