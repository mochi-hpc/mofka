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

TEST_CASE("Ack-early producer/consumer test", "[ack-early]") {

    spdlog::set_level(spdlog::level::from_str("critical"));
    auto remove_file = EnsureFileRemoved{"mofka.json"};

    auto server = bedrock::Server("na+sm", config);
    ENSURE(server.finalize());
    auto engine = server.getMargoManager().getThalliumEngine();

    SECTION("Produce with ack_early, consume and verify") {
        diaspora::Metadata options;
        options.json()["group_file"] = "mofka.json";
        options.json()["margo"] = nlohmann::json::object();
        options.json()["margo"]["use_progress_thread"] = true;
        diaspora::Driver driver = diaspora::Driver::New("mofka", options);
        REQUIRE(static_cast<bool>(driver));

        REQUIRE_NOTHROW(driver.createTopic("mytopic"));

        // Use default partition with ack_early enabled
        mofka::MofkaDriver::Dependencies partition_dependencies = {
            {"abt_io", {"my_abt_io"}}
        };
        diaspora::Metadata partition_config{
            R"({"path":"/tmp/mofka-ack-early-test","ack_early":{"enabled":true,"max_pending_batches":4}})"};

        REQUIRE_NOTHROW(driver.as<mofka::MofkaDriver>().addCustomPartition(
                    "mytopic", 0, "default",
                    partition_config, partition_dependencies));

        diaspora::TopicHandle topic;
        REQUIRE_NOTHROW(topic = driver.openTopic("mytopic"));
        REQUIRE(static_cast<bool>(topic));

        // Produce 100 events with ack_early option
        {
            std::vector<std::string> data(100);
            diaspora::Metadata producer_options;
            producer_options.json()["ack_early"] = true;
            auto producer = topic.producer(
                "myproducer", driver.defaultThreadPool(), producer_options);
            REQUIRE(static_cast<bool>(producer));
            for(unsigned i = 0; i < 100; ++i) {
                diaspora::Metadata metadata = diaspora::Metadata{
                    fmt::format("{{\"event_num\":{}}}", i)
                };
                data[i] = fmt::format("This is ack_early data for event {}", i);
                auto future = producer.push(
                    metadata,
                    diaspora::DataView{data[i].data(), data[i].size()});
            }
            producer.flush().wait(-1);
        }

        // Consume and verify all 100 events
        {
            diaspora::DataAllocator data_allocator =
                    [](const diaspora::Metadata&, const diaspora::DataDescriptor& descriptor) {
                auto size = descriptor.size();
                auto buf = new char[size];
                return diaspora::DataView{buf, size};
            };
            diaspora::DataSelector data_selector =
                [](const diaspora::Metadata&, const diaspora::DataDescriptor& descriptor) {
                    return descriptor;
                };
            auto consumer = topic.consumer(
                "myconsumer",
                data_selector,
                data_allocator);
            REQUIRE(static_cast<bool>(consumer));
            for(unsigned i = 0; i < 100; ++i) {
                auto opt_event = consumer.pull().wait(-1);
                REQUIRE(opt_event.has_value());
                auto& event = opt_event.value();
                REQUIRE(event.id() == i);
                auto& doc = event.metadata().json();
                REQUIRE(doc["event_num"].get<int64_t>() == i);
                REQUIRE(event.data().segments().size() == 1);
                auto data_str = std::string{
                    (const char*)event.data().segments()[0].ptr,
                    event.data().segments()[0].size};
                std::string expected = fmt::format("This is ack_early data for event {}", i);
                REQUIRE(data_str == expected);
                delete[] static_cast<const char*>(event.data().segments()[0].ptr);
            }
        }

    }
}
