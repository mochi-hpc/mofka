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

TEST_CASE("Eventbridge validation", "[eventbridge]") {

    auto partition_type = GENERATE(as<std::string>{}, "memory");
    CAPTURE(partition_type);
    auto remove_file = EnsureFileRemoved{"mofka.json"};

    auto server = bedrock::Server("na+sm", config);
    ENSURE(server.finalize());
    auto engine = server.getMargoManager().getThalliumEngine();

    SECTION("Initialize client/topic/producer") {

        auto client = mofka::Client{engine};
        REQUIRE(static_cast<bool>(client));
        auto sh = client.connect("mofka.json");
        REQUIRE(static_cast<bool>(sh));
        mofka::TopicHandle topic;
        REQUIRE(!static_cast<bool>(topic));
        mofka::Validator validator = mofka::Validator::FromMetadata(
            "eventbridge",
            mofka::Metadata{R"({"schema":{
                "a1": "a_value",
                "a2": ["a2_value_1", "a2_value_2"],

                "b1": [{"anything-but": "b1_value"}],
                "b2": [{"anything-but": ["b2_value_1", "b2_value_2"]}],
                "b3": [{"anything-but": {"prefix": "b3_prefix"}}],
                "b4": [{"anything-but": {"prefix": ["b4_prefix_1", "b4_prefix_2"]}}],
                "b5": [{"anything-but": {"prefix": {"equals-ignore-case": "b5_prefix" }}}],
                "b6": [{"anything-but": {"prefix": {"equals-ignore-case": ["b6_prefix_1", "b6_prefix_2"] }}}],
                "b7": [{"anything-but": {"suffix": "b7_suffix"}}],
                "b8": [{"anything-but": {"suffix": ["b8_suffix_1", "b8_suffix_2"]}}],
                "b9": [{"anything-but": {"suffix": {"equals-ignore-case": "b9_suffix" }}}],
                "b10": [{"anything-but": {"suffix": {"equals-ignore-case": ["b10_suffix_1", "b10_suffix_2"]}}}],

                "c1": [{"prefix": "c1_prefix"}],
                "c2": [{"prefix": ["c2_prefix_1", "c2_prefix_2"]}],
                "c3": [{"prefix": {"equals-ignore-case": "c3_prefix" }}],
                "c4": [{"prefix": {"equals-ignore-case": ["c4_prefix_1", "c4_prefix_2"] }}],

                "d1": [{"suffix": "d1_suffix"}],
                "d2": [{"suffix": ["d2_suffix_1", "d2_suffix_2"]}],
                "d3": [{"suffix": {"equals-ignore-case": "d3_suffix" }}],
                "d4": [{"suffix": {"equals-ignore-case": ["d4_suffix_1", "d4_suffix_2"] }}],

                "e1": [{"numeric": ["<", 42, ">=", 10]}],

                "f1": [{"exists": true}],
                "f2": [{"exists": false}],

                "g1": [{"equals-ignore-case": "g1_VAluE_1"}],
                "g2": [{"equals-ignore-case": ["g2_VAluE_1", "g2_VAluE_2"]}],

                "h1": [{"wildcard": "abc*def*ghi"}],

                "i1": {
                    "i1_1": "i1_1_value"
                },
                "i2.i2_1": "i2_1_value"
            }})"});
        REQUIRE_NOTHROW(sh.createTopic("mytopic", validator));

        mofka::Metadata partition_config;
        mofka::ServiceHandle::PartitionDependencies partition_dependencies;
        getPartitionArguments(partition_type, partition_dependencies, partition_config);

        REQUIRE_NOTHROW(sh.addCustomPartition(
                    "mytopic", 0, partition_type,
                    partition_config, partition_dependencies));
        REQUIRE_NOTHROW(topic = sh.openTopic("mytopic"));
        REQUIRE(static_cast<bool>(topic));

        auto thread_count = mofka::ThreadCount{0};
        auto batch_size   = mofka::BatchSize::Adaptive();
        auto ordering     = mofka::Ordering::Strict;

        auto producer = topic.producer(
            "myproducer", batch_size, thread_count, ordering);
        REQUIRE(static_cast<bool>(producer));
    }
}
