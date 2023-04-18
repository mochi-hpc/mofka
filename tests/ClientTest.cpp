/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <mofka/Client.hpp>
#include <mofka/Provider.hpp>
#include <mofka/TopicHandle.hpp>
#include <mofka/Admin.hpp>

static constexpr const char* topic_config = "{ \"path\" : \"mydb\" }";
static const std::string topic_type = "dummy";

TEST_CASE("Client test", "[client]") {

    auto engine = thallium::engine("na+sm", THALLIUM_SERVER_MODE);
    // Initialize the provider
    mofka::Provider provider(engine);
    mofka::Admin admin(engine);
    std::string addr = engine.self();
    auto topic_id = admin.createTopic(addr, 0, topic_type, topic_config);

    SECTION("Open topic") {
        mofka::Client client(engine);
        std::string addr = engine.self();

        mofka::TopicHandle my_topic = client.makeTopicHandle(addr, 0, topic_id);
        REQUIRE(static_cast<bool>(my_topic));

        auto bad_id = mofka::UUID::generate();
        REQUIRE_THROWS_AS(client.makeTopicHandle(addr, 0, bad_id), mofka::Exception);
    }

    admin.destroyTopic(addr, 0, topic_id);
    engine.finalize();
}
