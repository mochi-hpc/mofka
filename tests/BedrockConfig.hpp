/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <string>
#include <cstdio>

static inline const char* config = R"(
{
    "libraries" : {
        "mofka" : "libmofka-bedrock-module.so"
    },
    "providers" : [
        {
            "name" : "my_mofka_provider",
            "type" : "mofka",
            "provider_id" : 0
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

struct EnsureFileRemoved {

    std::string m_filename;

    template<typename ... Args>
    EnsureFileRemoved(Args&&... args)
    : m_filename(std::forward<Args>(args)...) {}

    ~EnsureFileRemoved() {
        std::remove(m_filename.c_str());
    }
};
