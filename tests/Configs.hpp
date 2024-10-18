/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <string>
#include <cstdio>
#include "mofka/MofkaDriver.hpp"

static inline const char* config = R"(
{
    "libraries" : [
        "libmofka-bedrock-module.so",
        "libflock-bedrock-module.so",
        "libwarabi-bedrock-module.so",
        "libyokan-bedrock-module.so"
    ],
    "providers" : [
        {
            "name" : "my_flock_provider",
            "type" : "flock",
            "provider_id" : 1,
            "config": {
                "bootstrap": "self",
                "file": "mofka.json",
                "group": {
                    "type": "static"
                }
            }
        },
        {
            "name" : "my_warabi_provider",
            "type" : "warabi",
            "provider_id" : 2,
            "config" : {
                "target" : {
                    "type": "memory",
                    "config": {}
                }
            },
            "tags": ["mofka:data"]
        },
        {
            "name" : "my_yokan_provider",
            "type" : "yokan",
            "provider_id" : 3,
            "tags" : [ "mofka:master", "mofka:metadata" ],
            "config" : {
                "database" : {
                    "type": "map",
                    "config": {}
                }
            }
        }
    ]
}
)";

static inline void getPartitionArguments(
        std::string_view partition_type,
        mofka::MofkaDriver::PartitionDependencies& dependencies,
        mofka::Metadata& partition_config) {
    if(partition_type == "memory") {
        dependencies = mofka::MofkaDriver::PartitionDependencies{};
        partition_config = mofka::Metadata{"{}"};
    } else if(partition_type == "default") {
        dependencies = {
            {"data", {"my_warabi_provider@local"}},
            {"metadata", {"my_yokan_provider@local"}}
        };
        partition_config = mofka::Metadata{"{}"};
    }
}
