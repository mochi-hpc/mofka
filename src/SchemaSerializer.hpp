/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_SCHEMA_SERIALIZER_H
#define MOFKA_SCHEMA_SERIALIZER_H

#include "JsonUtil.hpp"
#include "mofka/Serializer.hpp"
#include "mofka/Json.hpp"
#include <functional>

namespace mofka {

class SchemaSerializer : public SerializerInterface {

    using json = nlohmann::json;
    using SerializeFn = std::function<void(Archive&, const json&)>;
    using DeserializeFn = std::function<void(Archive&, json&)>;

    SerializeFn   m_serialize;
    DeserializeFn m_deserialize;
    json          m_json_schema;

    public:

    SchemaSerializer(
            std::function<void(Archive&, const json&)> s,
            std::function<void(Archive&, json&)>       d,
            json schema)
    : m_serialize(std::move(s))
    , m_deserialize(std::move(d))
    , m_json_schema(std::move(schema)) {}

    void serialize(Archive& archive, const Metadata& metadata) const override {
        m_serialize(archive, metadata.json());
    }

    void deserialize(Archive& archive, Metadata& metadata) const override {
        json j;
        m_deserialize(archive, j);
        metadata = mofka::Metadata{std::move(j)};
    }

    Metadata metadata() const override {
        auto config = json::object();
        config["type"] = "schema";
        config["schema"] = m_json_schema;
        return Metadata{std::move(config)};
    }

    static std::unique_ptr<SerializerInterface> create(const Metadata& metadata) {
        if(!metadata.isValidJson())
            throw Exception{"Provided Metadata is not valid JSON"};
        if(!metadata.json().contains("schema"))
            throw Exception{"SchemaSerializer is expecting a \"schema\" entry in its configuration"};
        // check that the schema field indeed is a schema
        JsonSchemaValidator{metadata.json()["schema"]};
        // build the schema serializer and deserialize
        auto serializer   = makeSerializer(metadata.json()["schema"]);
        auto deserializer = makeDeserializer(metadata.json()["schema"]);
        return std::make_unique<SchemaSerializer>(
                std::move(serializer),
                std::move(deserializer),
                metadata.json()["schema"]);
    }

    static SerializeFn makeSerializer(const json& schema) {
#if 0
        if(!schema.contains("type")) {
            auto all_types = json::array();
            all_types.push_back("null");
            all_types.push_back("boolean");
            all_types.push_back("integer");
            all_types.push_back("number");
            all_types.push_back("string");
            all_types.push_back("array");
            all_types.push_back("object");
            auto all_types_schema = json::object();
            all_types_schema["type"] = all_types;
            return makeSerializer(all_types_schema);
        }
        auto& type = schema["type"];
        if(type.is_array()) {
            return [](Archive& ar, const json& value) {
                char t = static_cast<char>(value.type());
                ar.write(&t, sizeof(t));
                // TODO
            };
        }
        if(type.is_string()) {
            if(type == "boolean") {
                return [](Archive& ar, const json& value) {
                    ar.write(&value.get_ref<const json::boolean_t&>(),
                             sizeof(json::boolean_t));
                };
            } else if(type == "null") {
                return [](Archive&, const json&) {};
            } else if(type == "integer") {
                return [](Archive& ar, const json& value) {
                    ar.write(&value.get_ref<const json::number_integer_t&>(),
                             sizeof(json::number_integer_t));
                };
            } else if(type == "number") {
                return [](Archive& ar, const json& value) {
                    if(value.is_number_float()) {
                        char t = 1;
                        ar.write(&t, sizeof(t));
                        ar.write(&value.get_ref<const json::number_float_t&>(),
                                 sizeof(json::number_float_t));
                    } else if(value.is_number_integer()) {
                        char t = 2;
                        ar.write(&t, sizeof(t));
                        ar.write(&value.get_ref<const json::number_integer_t&>(),
                                 sizeof(json::number_integer_t));
                    } else if(value.is_number_unsigned()) {
                        char t = 3;
                        ar.write(&t, sizeof(t));
                        ar.write(&value.get_ref<const json::number_unsigned_t&>(),
                                 sizeof(json::number_unsigned_t));
                    }
                };
            } else if(type == "string") {
                return [](Archive& ar, const json& value) {
                    auto& str = value.get_ref<const json::string_t&>();
                    size_t size = str.size();
                    ar.write(&size, sizeof(size));
                    ar.write(value.get_ref<const json::string_t&>().data(), size);
                };
            } else if(type == "array") {
                if(!schema.contains("items"))
                    return [](Archive&, const json&) {};
                auto& items = schema["items"];
                if(items.is_object()) { // items is a single schema
                    auto item_serializer_fn = makeSerializer(items);
                    return [item_ser_fn=std::move(item_serializer_fn)](Archive& ar, const json& value) {
                        auto& array = value.get_ref<const json::array_t&>();
                        size_t size = array.size();
                        ar.write(&size, sizeof(size));
                        for(size_t i=0; i < size; ++i)
                            item_ser_fn(ar, array[i]);
                    };
                } else { // items is an array of schemas
                    std::vector<SerializeFn> item_serializer_fns;
                    item_serializer_fns.reserve(items.size());
                    for(auto& item : items)
                        item_serializer_fns.push_back(makeSerializer(item));
                    return [item_ser_fns=std::move(item_serializer_fns),
                            extra_ser_fn = makeSerializer(json::object())](Archive& ar, const json& value) {
                        auto& array = value.get_ref<const json::array_t&>();
                        size_t size = array.size();
                        ar.write(&size, sizeof(size));
                        for(size_t i=0; i < size; ++i) {
                            if(i < item_ser_fns.size()) item_ser_fns[i](ar, array[i]);
                            else extra_ser_fn(ar, array[i]);
                        }
                    };
                }
            } else if(type == "object") {
                // TODO
            }
        }
        return {};
#endif
        (void)schema;
        return [](Archive& ar, const json& value) {
            auto str = value.dump();
            size_t s = str.size();
            ar.write(&s, sizeof(s));
            ar.write(str.data(), str.size());
        };
    }

    static DeserializeFn makeDeserializer(const json& schema) {
        // TODO
        (void)schema;
        return [](Archive& ar, json& value) {
            size_t s;
            ar.read(&s, sizeof(s));
            std::string str;
            str.resize(s);
            ar.read(const_cast<char*>(str.data()), str.size());
            value = json::parse(str);
        };
    }

};

}

#endif
