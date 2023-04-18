/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

static inline const char* config = R"(
{
    "libraries" : {
        "mofka" : "libmofka-bedrock-module.so"
    },
    "providers" : [
        {
            "name" : "my_mofka_provider",
            "type" : "mofka"
        }
    ]
}
)";

