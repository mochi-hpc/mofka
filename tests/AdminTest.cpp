/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <mofka/Admin.hpp>
#include <mofka/Provider.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>

static const std::string topic_type = "dummy";
static constexpr const char* topic_config = "{ \"path\" : \"mydb\" }";

TEST_CASE("Admin tests", "[admin]") {

    auto engine = thallium::engine("na+sm", THALLIUM_SERVER_MODE);
    // Initialize the provider
    mofka::Provider provider(engine);

    SECTION("Create an admin") {
        mofka::Admin admin(engine);
        std::string addr = engine.self();

        SECTION("Create and destroy topics") {
            mofka::UUID topic_id = admin.createTopic(addr, 0, topic_type, topic_config);

            REQUIRE_THROWS_AS(admin.createTopic(addr, 0, "blabla", topic_config),
                              mofka::Exception);

            admin.destroyTopic(addr, 0, topic_id);

            mofka::UUID bad_id;
            REQUIRE_THROWS_AS(admin.destroyTopic(addr, 0, bad_id), mofka::Exception);
        }
    }
    // Finalize the engine
    engine.finalize();
}
