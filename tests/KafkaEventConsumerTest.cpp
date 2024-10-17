/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <bedrock/Server.hpp>
#include <mofka/KafkaDriver.hpp>
#include <mofka/TopicHandle.hpp>
#include "Ensure.hpp"
#include <fstream>

static size_t topic_num = 0;

TEST_CASE("KafkaConsumer test", "[kafka-consumer]") {

    spdlog::set_level(spdlog::level::from_str("error"));

    nlohmann::json config = nlohmann::json::object();
    config["bootstrap.servers"] = "localhost:9092";
    {
        std::ofstream f{"kafka.json"};
        f << config.dump();
    }
    auto remove_file = EnsureFileRemoved{"kafka.json"};

    SECTION("Initialize a KafkaDriver, TopicHandle, and Producer") {

        mofka::KafkaDriver driver;
        REQUIRE_NOTHROW(driver = mofka::KafkaDriver{"kafka.json"});
        REQUIRE(static_cast<bool>(driver));

        mofka::TopicHandle topic;
        std::string topic_name = "mytopic_" + std::to_string(topic_num);
        topic_num += 1;
        REQUIRE_NOTHROW(driver.createTopic(topic_name));
        REQUIRE_NOTHROW(topic = driver.openTopic(topic_name));
        REQUIRE(static_cast<bool>(topic));

        {
            mofka::Producer producer;
            REQUIRE_NOTHROW(producer = topic.producer("myproducer", mofka::ThreadCount{0}));
            REQUIRE(static_cast<bool>(producer));
            {
                std::vector<std::string> data(100);
                for(size_t i = 0; i < 100; ++i) {
                    mofka::Future<mofka::EventID> future;
                    auto metadata = mofka::Metadata{
                        fmt::format("{{\"event_num\":{}}}", i)
                    };
                    data[i] = fmt::format("This is some data for event {}", i);
                    REQUIRE_NOTHROW(future = producer.push(
                                metadata,
                                mofka::Data{data[i].data(), data[i].size()}));
                }
                REQUIRE_NOTHROW(producer.flush());
            }
        }
        topic.markAsComplete();

        SECTION("Consumer without data")
        {
            mofka::Consumer consumer;
            REQUIRE_NOTHROW(consumer = topic.consumer("myconsumer", mofka::ThreadCount{0}));
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
                    auto& doc = metadata.json();
                    auto event_id = doc["event_num"].get<int64_t>();
                    if(event_id % 2 == 0) {
                        return descriptor;
                    } else {
                        return mofka::DataDescriptor::Null();
                    }
                };
            mofka::DataBroker data_broker =
                [](const mofka::Metadata& metadata, const mofka::DataDescriptor& descriptor) {
                    auto size = descriptor.size();
                    auto& doc = metadata.json();
                    auto event_id = doc["event_num"].get<int64_t>();
                    if(event_id % 2 == 0) {
                        auto data = new char[size];
                        return mofka::Data{data, size};
                    } else {
                        return mofka::Data{};
                    }
                    return mofka::Data{};
                };
            auto consumer = topic.consumer(
                    "myconsumer", data_selector, data_broker);
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
                    REQUIRE(event.data().segments().size() == 1);
                    auto data_str = std::string{
                        (const char*)event.data().segments()[0].ptr,
                            event.data().segments()[0].size};
                    std::string expected = fmt::format("This is some data for event {}", i);
                    REQUIRE(data_str == expected);
                    delete[] static_cast<const char*>(event.data().segments()[0].ptr);
                } else {
                    REQUIRE(event.data().segments().size() == 0);
                }
            }
            auto event = consumer.pull().wait();
            REQUIRE(event.id() == mofka::NoMoreEvents);
        }
    }
}
