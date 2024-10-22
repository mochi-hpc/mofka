/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <mofka/KafkaDriver.hpp>
#include <mofka/TopicHandle.hpp>
#include "Ensure.hpp"
#include <fstream>

static size_t topic_num = 0;

TEST_CASE("Event consumer test", "[event-consumer]") {

//    spdlog::set_level(spdlog::level::from_str("trace"));

    nlohmann::json config = nlohmann::json::object();
    config["bootstrap.servers"] = "localhost:9092";
    {
        std::ofstream f{"kafka.json"};
        f << config.dump();
    }
    auto remove_file = EnsureFileRemoved{"kafka.json"};

    auto driver = mofka::KafkaDriver{"kafka.json"};
    auto topic_name = fmt::format("mytopic_{}", topic_num);
    topic_num += 1;
    driver.createTopic(topic_name);

    sleep(1);

    auto topic = driver.openTopic(topic_name);

    std::string seg1 = "abcdefghijklmnopqrstuvwxyz";
    std::string seg2 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    // Producer
    {
        auto producer = topic.producer();
        REQUIRE(static_cast<bool>(producer));
        auto metadata = mofka::Metadata{};
        auto data = mofka::Data{{{seg1.data(), seg1.size()},{seg2.data(), seg2.size()}}};
        producer.push(metadata, data);
        producer.flush();
        sleep(1); // for some reason the producer's destructor hangs if we don't wait a bit after flush
    }
    topic.markAsComplete();

    SECTION("Consume no data") {
        mofka::DataSelector data_selector =
            [](const mofka::Metadata&, const mofka::DataDescriptor&) {
                return mofka::DataDescriptor::Null();
            };
        mofka::DataBroker data_broker =
            [](const mofka::Metadata&, const mofka::DataDescriptor&) {
                return mofka::Data{};
            };
        auto consumer = topic.consumer(
                "myconsumer", data_selector, data_broker);
        auto event = consumer.pull().wait();
        REQUIRE(event.data().size() == 0);
        REQUIRE(consumer.pull().wait().id() == mofka::NoMoreEvents);
    }

    SECTION("Consume the whole data") {
        mofka::DataSelector data_selector =
            [](const mofka::Metadata&, const mofka::DataDescriptor& descriptor) {
                return descriptor;
            };
        mofka::DataBroker data_broker =
            [](const mofka::Metadata&, const mofka::DataDescriptor& descriptor) {
                auto size = descriptor.size();
                auto data = new char[size];
                return mofka::Data{data, size};
            };
        auto consumer = topic.consumer(
                "myconsumer", data_selector, data_broker);
        auto event = consumer.pull().wait();
        REQUIRE(event.data().size() == 52);
        REQUIRE(event.data().segments().size() == 1);
        auto received = std::string_view{
            (const char*)event.data().segments()[0].ptr,
            event.data().segments()[0].size};
        REQUIRE(received == seg1+seg2);
        delete[] (char*)event.data().segments()[0].ptr;
        REQUIRE(consumer.pull().wait().id() == mofka::NoMoreEvents);
    }

    SECTION("Consume using makeSubView") {
        mofka::DataSelector data_selector =
            [](const mofka::Metadata&, const mofka::DataDescriptor& descriptor) {
                return descriptor.makeSubView(13, 26);
            };
        mofka::DataBroker data_broker =
            [](const mofka::Metadata&, const mofka::DataDescriptor& descriptor) {
                auto size = descriptor.size();
                auto data = new char[size];
                return mofka::Data{data, size};
            };
        auto consumer = topic.consumer(
                "myconsumer", data_selector, data_broker);
        auto event = consumer.pull().wait();
        REQUIRE(event.data().size() == 26);
        REQUIRE(event.data().segments().size() == 1);
        auto received = std::string_view{
            (const char*)event.data().segments()[0].ptr,
            event.data().segments()[0].size};
        REQUIRE(received == "nopqrstuvwxyzABCDEFGHIJKLM");
        delete[] (char*)event.data().segments()[0].ptr;
        REQUIRE(consumer.pull().wait().id() == mofka::NoMoreEvents);
    }

    SECTION("Consume using makeStridedView") {
        mofka::DataSelector data_selector =
            [](const mofka::Metadata&, const mofka::DataDescriptor& descriptor) {
                return descriptor.makeStridedView(13, 3, 4, 2);
            };
        mofka::DataBroker data_broker =
            [](const mofka::Metadata&, const mofka::DataDescriptor& descriptor) {
                auto size = descriptor.size();
                auto data = new char[size];
                return mofka::Data{data, size};
            };
        auto consumer = topic.consumer(
                "myconsumer", data_selector, data_broker);
        auto event = consumer.pull().wait();
        REQUIRE(event.data().size() == 12);
        REQUIRE(event.data().segments().size() == 1);
        auto received = std::string_view{
            (const char*)event.data().segments()[0].ptr,
            event.data().segments()[0].size};
        REQUIRE(received == "nopqtuvwzABC");
        delete[] (char*)event.data().segments()[0].ptr;
        REQUIRE(consumer.pull().wait().id() == mofka::NoMoreEvents);
    }

    SECTION("Consume using makeUnstructuredView") {
        mofka::DataSelector data_selector =
            [](const mofka::Metadata&, const mofka::DataDescriptor& descriptor) {
                return descriptor.makeUnstructuredView({
                        {3, 6},
                        {15, 4},
                        {27, 8}
                });
            };
        mofka::DataBroker data_broker =
            [](const mofka::Metadata&, const mofka::DataDescriptor& descriptor) {
                auto size = descriptor.size();
                auto data = new char[size];
                return mofka::Data{data, size};
            };
        auto consumer = topic.consumer(
                "myconsumer", data_selector, data_broker);
        auto event = consumer.pull().wait();
        REQUIRE(event.data().size() == 18);
        REQUIRE(event.data().segments().size() == 1);
        auto received = std::string_view{
            (const char*)event.data().segments()[0].ptr,
            event.data().segments()[0].size};
        REQUIRE(received == "defghipqrsBCDEFGHI");
        delete[] (char*)event.data().segments()[0].ptr;
        REQUIRE(consumer.pull().wait().id() == mofka::NoMoreEvents);
    }
}
