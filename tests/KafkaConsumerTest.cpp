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

TEST_CASE("KafkaConsumer test", "[kafka-consumer]") {

    spdlog::set_level(spdlog::level::from_str("error"));

    nlohmann::json config = nlohmann::json::object();
    config["bootstrap.servers"] = "localhost:9092";
    {
        std::ofstream f{"kafka.json"};
        f << config.dump();
    }
    auto remove_file = EnsureFileRemoved{"kafka.json"};

    SECTION("Initialize a KafkaDriver") {

        mofka::KafkaDriver driver;
        REQUIRE_NOTHROW(driver = mofka::KafkaDriver{"kafka.json"});
        REQUIRE(static_cast<bool>(driver));

        REQUIRE_NOTHROW(driver.createTopic("mytopic"));
        mofka::TopicHandle topic;
        REQUIRE_NOTHROW(topic = driver.openTopic("mytopic"));
        REQUIRE(static_cast<bool>(topic));

        SECTION("Create a consumer from the topic") {
            mofka::Consumer consumer;
            REQUIRE(!static_cast<bool>(consumer));
            consumer = topic.consumer("myconsumer");
            REQUIRE(static_cast<bool>(consumer));
            REQUIRE(consumer.name() == "myconsumer");
            REQUIRE(static_cast<bool>(consumer.topic()));
            REQUIRE(consumer.topic().name() == "mytopic");
        }
    }
}
