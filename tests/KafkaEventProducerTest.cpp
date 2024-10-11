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
        try {
            driver.createTopic("mytopic");
        } catch(const mofka::Exception& ex) {
            std::string msg{ex.what()};
            if(msg.find("Topic already exists") == std::string::npos)
                throw;
        }
        REQUIRE_NOTHROW(topic = driver.openTopic("mytopic"));
        REQUIRE(static_cast<bool>(topic));

        auto thread_count = GENERATE(as<mofka::ThreadCount>{}, 0, 1, 2);
        auto batch_size   = GENERATE(mofka::BatchSize::Adaptive(), mofka::BatchSize::Adaptive());
        auto ordering     = GENERATE(mofka::Ordering::Strict, mofka::Ordering::Loose);

        mofka::Producer producer;
        REQUIRE_NOTHROW(producer = topic.producer("myproducer", batch_size, thread_count, ordering));
        REQUIRE(static_cast<bool>(producer));

        SECTION("Push events into the topic using the producer") {
            auto future = producer.push(
                mofka::Metadata("{\"name\":\"matthieu\"}"),
                mofka::Data{nullptr, 0});
            producer.flush();
            future.wait();
        }

        SECTION("Push events with data") {
            std::string someData = "This is some data";
            auto future = producer.push(
                mofka::Metadata("{\"name\":\"matthieu\"}"),
                mofka::Data{someData.data(), someData.size()});
            producer.flush();
            future.wait();
        }
    }
}
