/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <mofka/Client.hpp>
#include <mofka/Provider.hpp>

static const std::string topic_type = "dummy";
static constexpr const char* topic_config = "{ \"path\" : \"mydb\" }";

TEST_CASE("Topic test", "[topic]") {
    auto engine = thallium::engine("na+sm", THALLIUM_SERVER_MODE);
    mofka::Provider provider(engine);
    mofka::UUID topic_id;

    SECTION("Create TopicHandle") {
        mofka::Client client(engine);
        std::string addr = engine.self();

        auto rh = client.makeTopicHandle(addr, 0, topic_id);

        SECTION("Send Hello RPC") {
            REQUIRE_NOTHROW(rh.sayHello());
        }
        SECTION("Send Sum RPC") {
            int32_t result = 0;
            REQUIRE_NOTHROW(rh.computeSum(42, 51, &result));
            REQUIRE(result == 93);

            REQUIRE_NOTHROW(rh.computeSum(42, 51));

            mofka::AsyncRequest request;
            REQUIRE_NOTHROW(rh.computeSum(42, 52, &result, &request));
            REQUIRE_NOTHROW(request.wait());
            REQUIRE(result == 94);
        }

        auto bad_id = mofka::UUID::generate();
        REQUIRE_THROWS_AS(client.makeTopicHandle(addr, 0, bad_id),
                          mofka::Exception);

        REQUIRE_THROWS_AS(client.makeTopicHandle(addr, 1, topic_id),
                         std::exception);
        REQUIRE_NOTHROW(client.makeTopicHandle(addr, 0, bad_id, false));
        REQUIRE_NOTHROW(client.makeTopicHandle(addr, 1, topic_id, false));
    }

    engine.finalize();
}
