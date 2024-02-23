/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_JSON_UTIL_H
#define MOFKA_JSON_UTIL_H

#include <nlohmann/json.hpp>
#include <nlohmann/json-schema.hpp>
#include <fmt/format.h>
#include <vector>
#include <stack>
#include <string>
#include <string_view>

namespace mofka {

static inline bool ValidateIsJson(std::string_view json) {
    std::stack<char> brackets;
    bool insideString = false;
    bool escaped = false;

    for (char c : json) {
        if (!escaped) {
            if (c == '"') {
                insideString = !insideString;
            } else if (!insideString) {
                if (c == '{' || c == '[') {
                    brackets.push(c);
                } else if (c == '}' || c == ']') {
                    if (brackets.empty()) {
                        // Extra closing bracket encountered
                        return false;
                    }
                    char top = brackets.top();
                    brackets.pop();

                    if ((c == '}' && top != '{') || (c == ']' && top != '[')) {
                        // Mismatched brackets
                        return false;
                    }
                }
            }
        }

        if (c == '\\') {
            escaped = !escaped;
        } else {
            escaped = false;
        }
    }

    return brackets.empty();
}

struct JsonValidator {

    using ErrorList = std::vector<std::string>;

    nlohmann::json_schema::json_validator m_validator;

    JsonValidator(const nlohmann::json& schema) {
        m_validator.set_root_schema(schema);
    }

    JsonValidator(nlohmann::json&& schema) {
        m_validator.set_root_schema(std::move(schema));
    }

    JsonValidator(const char* schema) {
        m_validator.set_root_schema(nlohmann::json::parse(schema));
    }

    ErrorList validate(const char* config_str) const {
        return validate(nlohmann::json::parse(config_str));
    }

    struct ErrorHandler : public nlohmann::json_schema::basic_error_handler {

        ErrorList m_errors;

        void error(const nlohmann::json::json_pointer &pointer,
                   const nlohmann::json &instance,
                   const std::string &message) override {
            nlohmann::json_schema::basic_error_handler::error(pointer, instance, message);
            std::stringstream ss;
            ss << "'" << pointer << "' - '" << instance << "': " << message;
            m_errors.push_back(std::move(ss).str());
        }
    };

    ErrorList validate(const nlohmann::json& json) const {
        ErrorHandler err;
        m_validator.validate(json, err);
        return std::move(err).m_errors;
    }

};

}

#endif
