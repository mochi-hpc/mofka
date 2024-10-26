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

TEST_CASE("KafkaProducer test", "[kafka-producer]") {

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

        auto thread_count = GENERATE(as<mofka::ThreadCount>{}, 0, 1);//, 2);
        auto batch_size   = GENERATE(mofka::BatchSize::Adaptive(), 0, 10);
        auto ordering     = GENERATE(mofka::Ordering::Strict);//, mofka::Ordering::Loose);
        //auto thread_count = mofka::ThreadCount{0};
        //auto batch_size = mofka::BatchSize{10};
        //auto ordering = mofka::Ordering::Strict;

        mofka::Producer producer;
        REQUIRE_NOTHROW(producer = topic.producer("myproducer", thread_count, batch_size, ordering));
        REQUIRE(static_cast<bool>(producer));
        std::vector<std::string> data(100);
        {
            for(size_t i = 0; i < 100; ++i) {
                mofka::Future<mofka::EventID> future;
                auto metadata = mofka::Metadata{
                    fmt::format("{{\"event_num\":{}}}", i)
                };
                data[i] = fmt::format("This is some data for event {}", i);
                REQUIRE_NOTHROW(future = producer.push(
                            metadata,
                            mofka::Data{data[i].data(), data[i].size()}));
                if(batch_size == mofka::BatchSize::Adaptive() || batch_size == mofka::BatchSize{0}) {
                    if((i+1) % 5 == 0)
                        REQUIRE_NOTHROW(future.wait());
                } else {
                    if((i+1) % batch_size.value == 0)
                        REQUIRE_NOTHROW(future.wait());
                }
            }
            REQUIRE_NOTHROW(producer.flush());
        }

        topic.markAsComplete();
    }
}
