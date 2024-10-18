/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_METADATA_HPP
#define MOFKA_METADATA_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Exception.hpp>
#include <mofka/Json.hpp>

#include <string>
#include <string_view>
#include <memory>
#include <vector>

namespace mofka {

class MetadataImpl;

/**
 * @brief A Metadata is an object that encapsulates the metadata of an event.
 */
class Metadata {

    public:

    /**
     * @brief Constructor taking a string. The string will be moved
     * into the Metadata object, hence it is passed by value.
     *
     * @param json JSON string.
     * @param validate Validate that the string is actually JSON.
     *
     * Note: if validate is left to false, validation will happen
     * only when events are pushed into a producer and only if the topic's
     * Validator requires the Metadata to be valid JSON.
     */
    Metadata(std::string json = "{}", bool validate = false);
    Metadata(std::string_view json, bool validate = false);
    Metadata(const char* json, bool validate = false);

    /**
     * @brief Constructor taking an already formed JSON document.
     *
     * @param json
     */
    Metadata(nlohmann::json json);

    /**
     * @brief Copy-constructor.
     */
    Metadata(const Metadata&);

    /**
     * @brief Move-constructor.
     */
    Metadata(Metadata&&);

    /**
     * @brief Copy-assignment operator.
     */
    Metadata& operator=(const Metadata&);

    /**
     * @brief Move-assignment operator.
     */
    Metadata& operator=(Metadata&&);

    /**
     * @brief Destructor.
     */
    ~Metadata();

    /**
     * @brief Checks if the Metadata instance is valid.
     */
    operator bool() const;

    /**
     * @brief Checks if the content of the Metadata is valid JSON.
     * If the Metadata has been initialized from a JSON document,
     * the call will be trivial. Other, the Metadata's string will
     * be validated but not converted into JSON.
     */
    bool isValidJson() const;

    /**
     * @brief Returns the underlying JSON document.
     *
     * Note: if the Metadata has been constructed from a string,
     * this function will trigger its parsing into a JSON document.
     */
    const nlohmann::json& json() const;

    /**
     * @brief Returns the underlying JSON document.
     *
     * Note: if the Metadata has been constructed from a string,
     * this function will trigger its parsing into a JSON document.
     * The string representation will also be invalidated.
     */
    nlohmann::json& json();

    /**
     * @brief Returns the underlying string representation
     * of the Metadata.
     *
     * Note: if the Metadata has been constructed from a JSON document,
     * this function will trigger its serialization into a string.
     */
    const std::string& string() const;

    /**
     * @brief Returns the underlying string representation
     * of the Metadata.
     *
     * Note: if the Metadata has been constructed from a JSON document,
     * this function will trigger its serialization into a string.
     * The JSON representation will also be invalidated.
     */
    std::string& string();

    private:

    /**
     * @brief Constructor is private. Use one of the static functions
     * to create a valid Metadata object.
     *
     * @param impl Pointer to implementation.
     */
    Metadata(const std::shared_ptr<MetadataImpl>& impl);

    std::shared_ptr<MetadataImpl> self;

    template<typename A>
    friend void save(A& ar, const Metadata& metadata);

    template<typename A>
    friend void load(A& ar, Metadata& metadata);

    friend class Event;
};

}

#endif
