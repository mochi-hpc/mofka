/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "EventbridgeValidator.hpp"

#include <functional>
#include <regex>
#include <sstream>
#include <optional>

namespace mofka {

MOFKA_REGISTER_VALIDATOR(eventbridge, EventbridgeValidator);

using json = nlohmann::json;

// Convert a string to lower-case
static std::string toLower(const std::string& str, size_t offset = 0, size_t len = std::string::npos) {
    std::string lowerStr;
    for(size_t i = offset; i - offset < len && i < str.size(); ++i) {
        lowerStr += std::tolower(str[i]);
    }
    return lowerStr;
};

// Convert a list of conditions into a single one matching any of them
template<typename CondList>
static std::function<bool(const json&)> anyOf(CondList&& list) {
    return [list=std::move(list)](const json& dataValue) {
        for(auto& cond : list) {
            if(cond(dataValue)) return true;
        }
        return false;
    };
}

// Convert a list of conditions into a single one matching all of them
template<typename CondList>
static std::function<bool(const json&)> allOf(CondList&& list) {
    return [list=std::move(list)](const json& dataValue) {
        for(auto& cond : list) {
            if(!cond(dataValue)) {
                return false;
            }
        }
        return true;
    };
}

// Forward declaration
static std::function<bool(const json&)> matchJson(const json& pattern);
static std::function<bool(const json&)> matchRule(const json& rule);

// Helper function to handle "anything-but":
//
// { "anyhing-but" : patternValue }
//
// Returns a function that takes a JSON value and returns true if it does not match
// the patternValue. The patternValue should be either a primitive type, an array of
// primitive types, or a sub-pattern.
static std::function<bool(const json&)> matchAnythingBut(const json& patternValue) {
    if(patternValue.is_primitive())
        return [patternValue](const json& dataValue) {
            return dataValue != patternValue;
        };
    else if(patternValue.is_array()) {
        for(auto& pattern : patternValue) {
            if(!pattern.is_primitive())
                throw Exception{
                    "Invalid Eventbridge \"anything-but\" rule: "
                    "list should contain only primitive types"
                };
        }
        return [patternValue](const json& dataValue) {
            for (const auto& value : patternValue) {
                if (dataValue == value) {
                    return false;
                }
            }
            return true;
        };
    } else { // patternValue.is_object()
        return [func=matchRule(patternValue)](const json& dataValue) {
            return !func(dataValue);
        };
    }
}

// Extracts suffixes or prefixes subpatterns
static std::pair<bool,std::vector<std::string>> extractPrefixesOrSuffixes(
        const json& prefixPattern, const std::string& ruleName) {
    // we use the name prefix hereafter but we could also be in a {"suffix" ... }
    // pattern, since this function is used by both prefix and suffix.
    std::vector<std::string> prefixes;
    bool ignore_case = false;
    if(prefixPattern.is_string()) {
        // { "prefix" : "abc" }
        prefixes.push_back(prefixPattern.get<std::string>());
    } else if(prefixPattern.is_array()) {
        // { "prefix" : ["abc", "def"] }
        for(auto& prefix : prefixPattern) {
            if(!prefix.is_string())
                throw Exception{fmt::format(
                    "Invalid Eventbridge \"{}\" rule: "
                    "list of prefixes should contain only strings",
                    ruleName)};
            prefixes.push_back(prefix.get<std::string>());
        }
    } else if(prefixPattern.is_object()) {
        if(prefixPattern.size() != 1 || !prefixPattern.contains("equals-ignore-case"))
            throw Exception{fmt::format(
                "Invalid Eventbridge \"{}\" rule: "
                "only \"equals-ignore-case\" is allowed as sub-pattern",
                ruleName)};
        ignore_case = true;
        if(prefixPattern["equals-ignore-case"].is_string()) {
            // { "prefix" : { "equals-ignore-case" : "abc" }}
            prefixes.push_back(
                toLower(prefixPattern["equals-ignore-case"].get_ref<const std::string&>()));
        } else if(prefixPattern["equals-ignore-case"].is_array()) {
            // { "prefix" : { "equals-ignore-case" : [ "abc", "def" ] }}
            for(auto& prefix : prefixPattern["equals-ignore-case"]) {
                if(!prefix.is_string())
                    throw Exception{fmt::format(
                        "Invalid Eventbridge \"{}\" rule: "
                        "list in \"equals-ignore-case\" should contain only strings",
                        ruleName)};
                prefixes.push_back(toLower(prefix.get_ref<const std::string&>()));
            }
        }
    } else {
        throw Exception{fmt::format(
            "Invalid Eventbridge \"{}\" rule: "
            "pattern should be a string, a list, or an object",
            ruleName)};
    }
    return std::make_pair(ignore_case, std::move(prefixes));
}

// Helper function to handle "prefix":
//
// { "prefix" : "abc" }
// { "prefix" : ["abc", "def"] }
// { "prefix" : { "equals-ignore-case" : "abc" }}
// { "prefix" : { "equals-ignore-case" : [ "abc", "def" ] }}
//
// Returns a function that takes a JSON value and returns true if it starts with
// (one of) the given prefix, potentially ignoring the case.
static std::function<bool(const json&)> matchPrefix(const json& prefixPattern) {
    bool ignore_case;
    std::vector<std::string> prefixes;
    std::tie(ignore_case, prefixes) = extractPrefixesOrSuffixes(prefixPattern, "prefix");
    if(ignore_case) {
        return [prefixes=std::move(prefixes)](const json& dataValue) {
            if(!dataValue.is_string()) return false;
            const auto& strValue = dataValue.get_ref<const std::string&>();
            for(const auto& prefix : prefixes) {
                if(prefix.size() > strValue.size()) continue;
                if(toLower(strValue).compare(0, prefix.size(), prefix, 0) == 0) return true;
            }
            return false;
        };
    } else {
        return [prefixes=std::move(prefixes)](const json& dataValue) {
            if(!dataValue.is_string()) return false;
            const auto& strValue = dataValue.get_ref<const std::string&>();
            for(const auto& prefix : prefixes) {
                if(prefix.size() > strValue.size()) continue;
                if(strValue.compare(0, prefix.size(), prefix, 0) == 0) return true;
            }
            return false;
        };
    }
}

// Helper function to handle "suffix":
//
// { "suffix" : "abc" }
// { "suffix" : ["abc", "def"] }
// { "suffix" : { "equals-ignore-case" : "abc" }}
// { "suffix" : { "equals-ignore-case" : [ "abc", "def" ] }}
//
// Returns a function that takes a JSON value and returns true if it ends with
// (one of) the given suffix, potentially ignoring the case.
static std::function<bool(const json&)> matchSuffix(const json& suffixPattern) {
    bool ignore_case;
    std::vector<std::string> suffixes;
    std::tie(ignore_case, suffixes) = extractPrefixesOrSuffixes(suffixPattern, "suffix");
    if(ignore_case) {
        return [suffixes=std::move(suffixes)](const json& dataValue) {
            if(!dataValue.is_string()) return false;
            const auto& strValue = dataValue.get_ref<const std::string&>();
            for(const auto& suffix : suffixes) {
                if(suffix.size() > strValue.size()) continue;
                if(toLower(strValue).compare(
                    strValue.size() - suffix.size(),
                    suffix.size(), suffix, 0) == 0) return true;
            }
            return false;
        };
    } else {
        return [suffixes=std::move(suffixes)](const json& dataValue) {
            if(!dataValue.is_string()) return false;
            const auto& strValue = dataValue.get_ref<const std::string&>();
            for(const auto& suffix : suffixes) {
                if(suffix.size() > strValue.size()) continue;
                if(strValue.compare(
                    strValue.size() - suffix.size(),
                    suffix.size(), suffix, 0) == 0) return true;
            }
            return false;
        };
    }
}

// Helper function to handle "equals-ignore-case":
//
// { "equals-ignore-case" : "abc" }
// { "equals-ignore-case" : [ "abc", "def" ] }
//
// Returns a function that takes a JSON value and returns true if it is equal
// to (one of) the possible value(s), ignoring the case.
static std::function<bool(const json&)> matchEqualsIgnoreCase(const json& patternValue) {
    if(patternValue.is_string()) {
        return [expectedValue=toLower(patternValue.get_ref<const std::string&>())](const json& dataValue) {
            return dataValue.is_string() &&
                   toLower(dataValue.get_ref<const std::string&>()) == expectedValue;
        };
    } else if(patternValue.is_array()) {
        std::vector<std::string> possibleValues;
        possibleValues.reserve(patternValue.size());
        for(auto& value : patternValue) {
            if(!value.is_string())
                throw Exception{
                    "Invalid Eventbridge \"equals-ignore-case\" rule: "
                    "list should contain only strings",
                };
            possibleValues.push_back(toLower(value.get_ref<const std::string&>()));
        }
        return [possibleValues=std::move(possibleValues)](const json& dataValue) {
            if(!dataValue.is_string()) return false;
            auto lower = toLower(dataValue.get_ref<const std::string&>());
            for(const auto& value : possibleValues) {
                if(lower == value) return true;
            }
            return false;
        };
    } else {
        throw Exception{
            "Invalid Eventbridge \"equals-ignore-case\" rule: "
            "pattern should be a string or a list of strings"};
    }
}

// Helper function to handle "numeric"
//
// { "numeric": [ ">", 0, "<=", 5 ] }
//
// Returns a function that takes a JSON value (expected to be numeric)
// and checks whether the provided conditions are satisfied.
static std::function<bool(const json&)> matchNumeric(const json& patternValue) {
    // patternValue is expected to be an array of conditions
    if(!patternValue.is_array() && patternValue.size() % 2 == 0)
        throw Exception{
            "Invalid Eventbridge \"numeric\" rule: "
            "pattern should be a list of even number of elements"};
    std::vector<std::function<bool(const json&)>> conditions;
    conditions.reserve(patternValue.size()/2);
    for (size_t i = 0; i < patternValue.size(); i += 2) {
        if(!patternValue[i].is_string())
            throw Exception{
                "Invalid Eventbridge \"numeric\" rule: "
                "operator should be a string"};
        if(!patternValue[i+1].is_number())
            throw Exception{
                "Invalid Eventbridge \"numeric\" rule: "
                "operand should be a number"};
        const auto& op = patternValue[i].get_ref<const std::string&>();
        double val = patternValue[i + 1].get<double>();
        if (op == "=") {
            conditions.push_back(
                [val](const json& num) {
                    if(!num.is_number()) return false;
                    return num.get<double>() == val;
                }
            );
        } else if (op == ">") {
            conditions.push_back(
                [val](const json& num) {
                    if(!num.is_number()) return false;
                    return num.get<double>() > val;
                }
            );
        } else if (op == ">=") {
            conditions.push_back(
                [val](const json& num) {
                    if(!num.is_number()) return false;
                    return num.get<double>() >= val;
                }
            );
        } else if (op == "<") {
            conditions.push_back(
                [val](const json& num) {
                    if(!num.is_number()) return false;
                    return num.get<double>() < val;
                }
            );
        } else if (op == "<=") {
            conditions.push_back(
                [val](const json& num) {
                    if(!num.is_number()) return false;
                    return num.get<double>() <= val;
                }
            );
        } else {
            throw Exception{fmt::format(
                "Invalid Eventbridge \"numeric\" rule: "
                "unknown operator \"{}\"", op)};
        }
    }
    return allOf(std::move(conditions));
}

// Helper function to handle "$or":
//
// "$or": [
//      { "x": [ { "numeric": [ ">", 0, "<=", 5 ] } ] },
//      { "y": [ { "numeric": [ "<", 10 ] } ] },
//      { "z": [ { "numeric": [ "=", 3.018e2 ] } ] }
// ]
//
// The patternValue should be a list of objects
static std::function<bool(const json&)> matchOr(const json& patternValue) {
    if(!patternValue.is_array())
        throw Exception{
            "Invalid Eventbridge \"$or\" rule: "
            "pattern should be a list of objects"};
    std::vector<std::function<bool(const json&)>> orConditions;
    for (const auto& condition : patternValue) {
        if(!condition.is_object())
            throw Exception{
                "Invalid Eventbridge \"$or\" rule: "
                "pattern should be a list of objects"};
        orConditions.push_back(matchJson(condition));
    }
    return anyOf(std::move(orConditions));
}

// Helper function to handle "wildcard":
//
// { "wildcard": "abc*def" }
//
// Returns a function that takes a JSON value that is expected to be a string
// and match it against the specified wildcard pattern.
static std::function<bool(const json&)> matchWildcard(const json& wildcardPattern) {
    if(!wildcardPattern.is_string())
        throw Exception{
            "Invalid Eventbridge \"wildcard\" rule: "
            "pattern should be a string"};

    auto splitStringAtDoubleBackslash = [](const std::string &input) {
        std::vector<std::string> result;
        size_t start = 0;
        size_t end = input.find("\\\\");
        while (end != std::string::npos) {
            result.push_back(input.substr(start, end - start));
            start = end + 2;
            end = input.find("\\\\", start);
        }
        result.push_back(input.substr(start));
        return result;
    };

    auto splitPattern = splitStringAtDoubleBackslash(wildcardPattern.get_ref<const std::string&>());
    auto wildcardRegex = std::regex(R"(\*)");
    for(auto& segment : splitPattern)
        segment = std::regex_replace(segment, wildcardRegex, ".*");
    auto finalRegex = splitPattern[0];
    for(size_t i = 1; i < splitPattern.size(); i++) {
        finalRegex += "\\\\";
        finalRegex += splitPattern[i];
    }
    auto regex = std::regex(finalRegex);
    return [regex=std::move(regex)](const json& dataValue) {
        if (!dataValue.is_string()) return false;
        return std::regex_match(dataValue.get_ref<const std::string&>(), regex);
    };
}

// Helper function to extract value from nested JSON using dot-separated keys.
static std::optional<json> getNestedValue(const json& data, const std::string& key) {
    std::istringstream iss(key);
    std::string token;
    json current = data;
    while (std::getline(iss, token, '.')) {
        if (!current.contains(token)) {
            return std::nullopt;
        }
        current = current[token];
    }
    return current;
}

// Function called when encountering { "field" : "value" } to return a function
// that checks whether the field exists and has the expected value.
static std::function<bool(const json&)> matchKeyValue(const std::string& key, const json& expectedValue) {
    return [key, expectedValue](const json& data) {
        auto maybeValue = getNestedValue(data, key);
        if(maybeValue.has_value())
            return maybeValue.value() == expectedValue;
        else
            return false;
    };
}

// Match a rule. A rule is a one-key object placed within a list, in
// evenbridge format. The rule argument of this function is that object.
static std::function<bool(const json&)> matchRule(const json& rule) {
    std::function<bool(const json&)> func;
    if(rule.contains("anything-but")) {
        func = matchAnythingBut(rule["anything-but"]);
    } else if(rule.contains("prefix")) {
        func = matchPrefix(rule["prefix"]);
    } else if(rule.contains("suffix")) {
        func = matchSuffix(rule["suffix"]);
    } else if(rule.contains("equals-ignore-case")) {
        func = matchEqualsIgnoreCase(rule["equals-ignore-case"]);
    } else if(rule.contains("numeric")) {
        func = matchNumeric(rule["numeric"]);
    } else if(rule.contains("$or")) {
        func = matchOr(rule["$or"]);
    } else if(rule.contains("wildcard")) {
        func = matchWildcard(rule["wildcard"]);
    } else {
        throw Exception{fmt::format(
            "Invalid Eventbridge pattern: "
            "unexpected rule: {}", rule.dump())};
    }
    return func;
}

// Helper function to handle "exists"
static std::function<bool(const json&)> matchExists(const std::string& key, const json& shouldExist) {
    if(!shouldExist.is_boolean())
        throw Exception{"Invalid Eventbridge \"exists\" rule: expected boolean value"};
    return [shouldExist=shouldExist.get<bool>(), key=key](const json& dataValue) {
        return shouldExist == getNestedValue(dataValue, key).has_value();
    };
}

// Helper function that mathes a pattern. A pattern must be an object
// associating fields with patterns.
static std::function<bool(const json&)> matchJson(const json& pattern) {
    std::vector<std::function<bool(const json&)>> conditions;
    if(!pattern.is_object())
        throw Exception{"Invalid Eventbridge pattern: expected object"};
    for(const auto& [key, value] : pattern.items() ) {
        if(value.is_primitive()) {
            // "key": "expectedValue"
            conditions.push_back(matchKeyValue(key, value));
        } else if(value.is_object()) {
            // "key" : { subpatterns }
            conditions.push_back([key=key, inner_condition=matchJson(value)](const json& data) {
                auto maybeValue = getNestedValue(data, key);
                if(maybeValue.has_value())
                    return inner_condition(maybeValue.value());
                else
                    return false;
            });
        } else { // value.is_array()
            // "key" : [ rules ] or "key": [ possible values ]
            bool has_primitives = false;
            bool has_objects = false;
            // check whether we have a list of rules or a list of possible values (primitive)
            for(auto& rule : value) {
                if(rule.is_primitive()) {
                    has_primitives = true;
                    if(has_objects)
                        throw Exception{
                            "Invalid Eventbridge pattern: "
                            "cannot mix primitives with complex conditions"};
                } else if(rule.is_object()) {
                    has_objects = true;
                    if(has_primitives)
                        throw Exception{
                            "Invalid Eventbridge pattern: "
                            "cannot mix primitives with complex conditions"};
                    if(rule.size() != 1)
                        throw Exception{
                            "Invalid Eventbridge pattern: "
                            "each rule can only have one statement"};
                } else {
                    throw Exception{"Invalid Eventbridge pattern: expected object or primitive"};
                }
            }
            if(has_primitives) { // "key" : [ possible values ]
                std::vector<std::function<bool(const json&)>> possibleValueCheckers;
                for(const auto& possibleValue : value) {
                    possibleValueCheckers.push_back(matchKeyValue(key, possibleValue));
                }
                conditions.push_back(anyOf(possibleValueCheckers));
            } else { // "key": [ rules ]
                std::vector<std::function<bool(const json&)>> orConditions;
                for(const auto& rule : value) {
                    std::function<bool(const json&)> inner_condition;
                    if(!rule.contains("exists")) {
                        orConditions.push_back([func=matchRule(rule), key=key](const json& data) {
                            auto maybeValue = getNestedValue(data, key);
                            if(maybeValue.has_value())
                                return func(maybeValue.value());
                            else
                                return false;
                        });
                    } else {
                        orConditions.push_back(matchExists(key, rule["exists"]));
                    }
                }
                conditions.push_back(anyOf(orConditions));
            }
        }
    }
    return allOf(conditions);
}

void EventbridgeValidator::validate(const Metadata& metadata, const Data& data) const {
    (void)data;
    if(!metadata.isValidJson())
        throw InvalidMetadata("Metadata object does not contain valid JSON metadata");
    if(!m_validator(metadata.json()))
        throw InvalidMetadata("Metadata object does not satisfy eventbridge pattern");
}

Metadata EventbridgeValidator::metadata() const {
    json config = json::object();
    config["__type__"] = "eventbridge";
    config["schema"] = m_schema;
    return Metadata{std::move(config)};
}

std::unique_ptr<ValidatorInterface> EventbridgeValidator::create(const Metadata& metadata) {
    const auto& config = metadata.json();
    if(!config.contains("schema")) {
        throw InvalidMetadata{"Metadata object does not contain a \"schema\" field for EventbridgeValidator"};
    }
    const auto& schema = config["schema"];
    if(!schema.is_object()) {
        throw InvalidMetadata{"\"schema\" field in EventbridgeValidator configuration should be an object"};
    }
    return std::make_unique<EventbridgeValidator>(schema, matchJson(schema));
}

}
