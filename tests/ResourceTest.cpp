/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <alpha/Client.hpp>
#include <alpha/Provider.hpp>
#include <alpha/Admin.hpp>

static const std::string resource_type = "dummy";
static constexpr const char* resource_config = "{ \"path\" : \"mydb\" }";

TEST_CASE("Resource test", "[resource]") {
    auto engine = thallium::engine("na+sm", THALLIUM_SERVER_MODE);
    alpha::Admin admin(engine);
    alpha::Provider provider(engine);
    std::string addr = engine.self();
    auto resource_id = admin.createResource(addr, 0, resource_type, resource_config);

    SECTION("Create ResourceHandle") {
        alpha::Client client(engine);
        std::string addr = engine.self();

        auto rh = client.makeResourceHandle(addr, 0, resource_id);

        SECTION("Send Hello RPC") {
            REQUIRE_NOTHROW(rh.sayHello());
        }
        SECTION("Send Sum RPC") {
            int32_t result = 0;
            REQUIRE_NOTHROW(rh.computeSum(42, 51, &result));
            REQUIRE(result == 93);

            REQUIRE_NOTHROW(rh.computeSum(42, 51));

            alpha::AsyncRequest request;
            REQUIRE_NOTHROW(rh.computeSum(42, 52, &result, &request));
            REQUIRE_NOTHROW(request.wait());
            REQUIRE(result == 94);
        }

        auto bad_id = alpha::UUID::generate();
        REQUIRE_THROWS_AS(client.makeResourceHandle(addr, 0, bad_id),
                          alpha::Exception);

        REQUIRE_THROWS_AS(client.makeResourceHandle(addr, 1, resource_id),
                         std::exception);
        REQUIRE_NOTHROW(client.makeResourceHandle(addr, 0, bad_id, false));
        REQUIRE_NOTHROW(client.makeResourceHandle(addr, 1, resource_id, false));
    }

    admin.destroyResource(addr, 0, resource_id);
    engine.finalize();
}
