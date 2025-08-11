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

TEST_CASE("Eventbridge validation", "[eventbridge]") {

    auto partition_type = GENERATE(as<std::string>{}, "memory");
    CAPTURE(partition_type);
    auto remove_file = EnsureFileRemoved{"mofka.json"};

    auto server = bedrock::Server("na+sm", config);
    ENSURE(server.finalize());
    auto engine = server.getMargoManager().getThalliumEngine();

    SECTION("Initialize driver/topic/producer") {

        diaspora::Metadata options;
        options.json()["group_file"] = "mofka.json";
        options.json()["margo"] = nlohmann::json::object();
        options.json()["margo"]["use_progress_thread"] = true;
        diaspora::Driver driver = diaspora::Driver::New("mofka", options);
        REQUIRE(static_cast<bool>(driver));

        diaspora::TopicHandle topic;
        REQUIRE(!static_cast<bool>(topic));
        diaspora::Validator validator = diaspora::Validator::FromMetadata(
            diaspora::Metadata{R"({"type":"eventbridge", "schema":{
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
                "e2": [{"numeric": ["<=", 42, ">", 10]}],

                "f1": [{"exists": true}],
                "f2": [{"exists": false}],

                "g1": [{"equals-ignore-case": "g1_VAluE_1"}],
                "g2": [{"equals-ignore-case": ["g2_VAluE_1", "g2_VAluE_2"]}],

                "h1": [{"wildcard": "abc*def*ghi"}],

                "i1": {
                    "i1_1": "i1_1_value"
                },
                "i2.i2_1": "i2_1_value",

                "j1": [{
                    "$or": [
                        { "j1_1" : [{"exists": true}]},
                        { "j2_2" : [{"exists": true}]}
                    ]}]
            }})"});
        REQUIRE_NOTHROW(driver.createTopic("mytopic", diaspora::Metadata{}, validator));

        // example of metadata that matches the pattern
        diaspora::Metadata example{R"({
            "a1": "a_value",
            "a2": "a2_value_2",
            "b1": "not_b1_value",
            "b2": "not_b2_value_1",
            "b3": "not_b3_prefix_value",
            "b4": "not_b4_prefix_1",
            "b5": "not_b5_PREfix",
            "b6": "not_b6_PREfix_1",
            "b7": "b7_suffix_not",
            "b8": "b8_suffix_1_not",
            "b9": "b9_SUFfix_not",
            "b10": "b10_SUFfix_1_not",
            "c1": "c1_prefix_value",
            "c2": "c2_prefix_1_value",
            "c3": "c3_PREfix_value",
            "c4": "c4_PREfix_1_value",
            "d1": "value_d1_suffix",
            "d2": "value_d2_suffix_2",
            "d3": "value_d3_SUFfix",
            "d4": "value_d4_SUFfix_2",
            "e1": 33,
            "e2": 34,
            "f1": "something",
            "g1": "g1_vaLUe_1",
            "g2": "g2_vaLUe_2",
            "h1": "abcXXXdefYYghi",
            "i1": {
                "i1_1": "i1_1_value"
            },
            "i2": {
                "i2_1": "i2_1_value"
            },
            "j1": { "j2_2": 123 }
        })"};

        diaspora::Metadata partition_config;
        mofka::MofkaDriver::Dependencies partition_dependencies;
        getPartitionArguments(partition_type, partition_dependencies, partition_config);

        REQUIRE_NOTHROW(driver.as<mofka::MofkaDriver>().addCustomPartition(
                    "mytopic", 0, partition_type,
                    partition_config, partition_dependencies));
        REQUIRE_NOTHROW(topic = driver.openTopic("mytopic"));
        REQUIRE(static_cast<bool>(topic));

        auto thread_count = diaspora::ThreadCount{0};
        auto batch_size   = diaspora::BatchSize::Adaptive();
        auto ordering     = diaspora::Ordering::Strict;

        auto producer = topic.producer(
            "myproducer", batch_size, thread_count, ordering);
        REQUIRE(static_cast<bool>(producer));

        REQUIRE_NOTHROW(producer.push(example).wait());
    }
}
