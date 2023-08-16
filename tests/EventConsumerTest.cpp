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
#include "BedrockConfig.hpp"

TEST_CASE("Event consumer test", "[event-consumer]") {

    spdlog::set_level(spdlog::level::from_str("critical"));
    auto remove_file = EnsureFileRemoved{"mofka.ssg"};

    auto server = bedrock::Server("na+sm", config);
    auto gid = server.getSSGManager().getGroup("mofka_group")->getHandle<uint64_t>();
    auto engine = server.getMargoManager().getThalliumEngine();

    SECTION("Producer/consumer") {
        auto client = mofka::Client{engine};
        REQUIRE(static_cast<bool>(client));
        auto sh = client.connect(mofka::SSGGroupID{gid});
        REQUIRE(static_cast<bool>(sh));
        mofka::TopicHandle topic;
        REQUIRE(!static_cast<bool>(topic));
        auto topic_config = mofka::TopicBackendConfig{"{\"__type__\":\"dummy\"}"};
        topic = sh.createTopic("mytopic", topic_config);
        REQUIRE(static_cast<bool>(topic));

        {
            auto producer = topic.producer();
            REQUIRE(static_cast<bool>(producer));
            for(unsigned i=0; i < 100; ++i) {
                mofka::Metadata metadata = mofka::Metadata{
                    fmt::format("{{\"event_num\":{}}}", i)
                };
                std::string data = fmt::format("This is data for event {}", i);
                auto future = producer.push(
                    metadata,
                    mofka::Data{data.data(), data.size()});
                future.wait();
            }
        }

        SECTION("Consumer without data")
        {
            auto consumer = topic.consumer("myconsumer");
            REQUIRE(static_cast<bool>(consumer));
            for(unsigned i=0; i < 100; ++i) {
                auto event = consumer.pull().wait();
                REQUIRE(event.id() == i);
                auto& doc = event.metadata().json();
                REQUIRE(doc["event_num"].GetInt64() == i);
                if(i % 5 == 0)
                    event.acknowledge();
            }
        }

        SECTION("Consume with data")
        {
            mofka::DataSelector data_selector = [](const mofka::Metadata& metadata, const mofka::DataDescriptor& descriptor) {
                auto& doc = metadata.json();
                auto event_id = doc["event_num"].GetInt64();
                std::cerr << "DataDescriptor for event " << event_id << " has size " << descriptor.size() << std::endl;
                if(event_id % 2 == 0) {
                    return descriptor;
                } else {
                    return mofka::DataDescriptor::Null();
                }
            };
            mofka::DataBroker data_broker = [](const mofka::Metadata& metadata, const mofka::DataDescriptor& descriptor) {
                auto size = descriptor.size();
                auto& doc = metadata.json();
                if(doc["event_num"].GetInt64() % 2 == 0) {
                    auto data = new char[size];
                    return mofka::Data{data, size};
                } else {
                    return mofka::Data{nullptr, 0};
                }
            };
            auto consumer = topic.consumer(
                "myconsumer", data_selector, data_broker);
            REQUIRE(static_cast<bool>(consumer));
            for(unsigned i=0; i < 100; ++i) {
                auto event = consumer.pull().wait();
                REQUIRE(event.id() == i);
                auto& doc = event.metadata().json();
                REQUIRE(doc["event_num"].GetInt64() == i);
                if(i % 5 == 0)
                    event.acknowledge();
                if(i % 2 == 0 && event.data().segments().size() == 1)
                    delete[] static_cast<const char*>(event.data().segments()[0].ptr);
            }
        }
    }

    server.finalize();
}
