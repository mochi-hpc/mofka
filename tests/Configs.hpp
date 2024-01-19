/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <string>
#include <cstdio>
#include "mofka/ServiceHandle.hpp"

static inline const char* config = R"(
{
    "libraries" : {
        "mofka" : "libmofka-bedrock-module.so",
        "warabi" : "libwarabi-bedrock-module.so",
        "yokan" : "libyokan-bedrock-module.so"
    },
    "providers" : [
        {
            "name" : "my_warabi_provider",
            "type" : "warabi",
            "provider_id" : 1,
            "config" : {
                "target" : {
                    "type": "memory",
                    "config": {}
                }
            }
        },
        {
            "name" : "my_yokan_provider",
            "type" : "yokan",
            "provider_id" : 2,
            "tags" : [ "mofka:master" ],
            "config" : {
                "database" : {
                    "type": "map",
                    "config": {}
                }
            }
        }
    ],
    "ssg" : [
        {
            "name" : "mofka_group",
            "method" : "init",
            "group_file" : "mofka.ssg",
            "swim" : {
                "period_length_ms" : 100
            }
        }
    ]
}
)";

static inline void getPartitionArguments(
        std::string_view partition_type,
        mofka::ServiceHandle::PartitionDependencies& dependencies,
        mofka::Metadata& partition_config) {
    if(partition_type == "memory") {
        dependencies = mofka::ServiceHandle::PartitionDependencies{};
        partition_config = mofka::Metadata{"{}"};
    } else if(partition_type == "default") {
        dependencies = {
            {"data", {"my_warabi_provider@local"}},
            {"metadata", {"my_yokan_provider@local"}}
        };
        partition_config = mofka::Metadata{"{}"};
    }
}

struct EnsureFileRemoved {

    std::string m_filename;

    template<typename ... Args>
    EnsureFileRemoved(Args&&... args)
    : m_filename(std::forward<Args>(args)...) {}

    ~EnsureFileRemoved() {
        std::remove(m_filename.c_str());
    }
};
