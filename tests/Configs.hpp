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
            "name" : "my_yokan_master_provider",
            "type" : "yokan",
            "provider_id" : 3,
            "tags" : [ "mofka:master" ],
            "config" : {
                "database" : {
                    "type": "map",
                    "config": {}
                }
            }
        },
        {
            "name" : "my_yokan_metadata_provider",
            "type" : "yokan",
            "provider_id" : 4,
            "tags" : [ "mofka:metadata" ],
            "config" : {
                "database" : {
                    "type": "log",
                    "config": {
                        "path": "/tmp/mofka-logs/metadata-log"
                    }
                }
            }
        }
    ]
}
)";

static inline void getPartitionArguments(
        std::string_view partition_type,
        mofka::MofkaDriver::Dependencies& dependencies,
        mofka::Metadata& partition_config) {
    if(partition_type == "memory") {
        dependencies = mofka::MofkaDriver::Dependencies{};
        partition_config = mofka::Metadata{"{}"};
    } else if(partition_type == "default") {
        dependencies = {
            {"data", {"my_warabi_provider@local"}},
            {"metadata", {"my_yokan_metadata_provider@local"}}
        };
        partition_config = mofka::Metadata{"{}"};
    }
}
