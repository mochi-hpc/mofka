/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <bedrock/Server.hpp>
#include <mofka/Client.hpp>
#include <mofka/TopicHandle.hpp>
#include "Configs.hpp"
#include "Ensure.hpp"

TEST_CASE("Event consumer test", "[event-consumer]") {

//    spdlog::set_level(spdlog::level::from_str("trace"));
    auto partition_type = GENERATE(as<std::string>{}, "memory", "default");
    CAPTURE(partition_type);
    auto remove_file = EnsureFileRemoved{"mofka.json"};

    auto server = bedrock::Server("na+sm", config);
    ENSURE(server.finalize());
    auto engine = server.getMargoManager().getThalliumEngine();

    SECTION("Producer/consumer") {
        auto client = mofka::Client{engine};
        REQUIRE(static_cast<bool>(client));
        auto sh = client.connect("mofka.json");
        REQUIRE(static_cast<bool>(sh));
        mofka::TopicHandle topic;
        REQUIRE(!static_cast<bool>(topic));
        REQUIRE_NOTHROW(sh.createTopic("mytopic"));
        mofka::Metadata partition_config;
        mofka::ServiceHandle::PartitionDependencies partition_dependencies;
        getPartitionArguments(partition_type, partition_dependencies, partition_config);

        REQUIRE_NOTHROW(sh.addCustomPartition(
                    "mytopic", 0, partition_type,
                    partition_config, partition_dependencies));
        REQUIRE_NOTHROW(topic = sh.openTopic("mytopic"));
        REQUIRE(static_cast<bool>(topic));

        {
            auto producer = topic.producer();
            REQUIRE(static_cast<bool>(producer));
            for(unsigned i=0; i < 100; ++i) {
                mofka::Metadata metadata = mofka::Metadata{
                    fmt::format("{{\"event_num\":{}}}", i)
                };
                std::string data = fmt::format("This is data for event {}", i);
                mofka::Future<mofka::EventID> future;
                REQUIRE_NOTHROW(future = producer.push(
                    metadata,
                    mofka::Data{data.data(), data.size()}));
                REQUIRE_NOTHROW(future.wait());
            }
        }
        topic.markAsComplete();

        SECTION("Consumer without data")
        {
            mofka::Consumer consumer;
            REQUIRE_NOTHROW(consumer = topic.consumer("myconsumer"));
            REQUIRE(static_cast<bool>(consumer));
            for(unsigned i=0; i < 100; ++i) {
                mofka::Event event;
                REQUIRE_NOTHROW(event = consumer.pull().wait());
                REQUIRE(event.id() == i);
                auto& doc = event.metadata().json();
                REQUIRE(doc["event_num"].get<int64_t>() == i);
                if(i % 5 == 0)
                    REQUIRE_NOTHROW(event.acknowledge());
            }
            // Consume extra events, we should get events with NoMoreEvents as event IDs
            for(unsigned i=0; i < 10; ++i) {
                mofka::Event event;
                REQUIRE_NOTHROW(event = consumer.pull().wait());
                REQUIRE(event.id() == mofka::NoMoreEvents);
            }
        }

        SECTION("Consume with data")
        {
            mofka::DataSelector data_selector =
                    [](const mofka::Metadata& metadata, const mofka::DataDescriptor& descriptor) {
                        return mofka::DataDescriptor::Null();
#if 0
                auto& doc = metadata.json();
                auto event_id = doc["event_num"].get<int64_t>();
                if(event_id % 2 == 0) {
                    return descriptor;
                } else {
                    return mofka::DataDescriptor::Null();
                }
#endif
            };
            mofka::DataBroker data_broker =
                    [](const mofka::Metadata& metadata, const mofka::DataDescriptor& descriptor) {
#if 0
                auto size = descriptor.size();
                auto& doc = metadata.json();
                auto event_id = doc["event_num"].get<int64_t>();
                if(event_id % 2 == 0) {
                    auto data = new char[size];
                    return mofka::Data{data, size};
                } else {
                    return mofka::Data{};
                }
#endif
                return mofka::Data{};
            };
#if 0
            auto consumer = topic.consumer(
                "myconsumer", data_selector, data_broker);
#endif
            auto consumer = topic.consumer("myconsumer");
            REQUIRE(static_cast<bool>(consumer));
            for(unsigned i=0; i < 100; ++i) {
                mofka::Event event;
                REQUIRE_NOTHROW(event = consumer.pull().wait());
                REQUIRE(event.id() == i);
                auto& doc = event.metadata().json();
                REQUIRE(doc["event_num"].get<int64_t>() == i);
                if(i % 5 == 0)
                    REQUIRE_NOTHROW(event.acknowledge());
                if(i % 2 == 0) {
#if 0
                    REQUIRE(event.data().segments().size() == 1);
                    auto data_str = std::string{
                        (const char*)event.data().segments()[0].ptr,
                        event.data().segments()[0].size};
                    std::string expected = fmt::format("This is data for event {}", i);
                    REQUIRE(data_str == expected);
                    delete[] static_cast<const char*>(event.data().segments()[0].ptr);
#endif
                } else {
                    REQUIRE(event.data().segments().size() == 0);
                }
            }
            auto event = consumer.pull().wait();
            REQUIRE(event.id() == mofka::NoMoreEvents);
        }
    }
}
