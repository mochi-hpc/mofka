/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_METADATA_HPP
#define MOFKA_METADATA_HPP

#include <rapidjson/document.h>
#include <mofka/Exception.hpp>
#include <memory>

namespace mofka {

class MetadataImpl;

/**
 * @brief A Metadata is an object that encapsulates the metadata of an event.
 */
class Metadata {

    public:

    /**
     * @brief Constructor. The resulting Metadata handle will be invalid.
     */
    Metadata();

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
     * @brief Create a Metadata object from a JSON string.
     */
    static Metadata FromJSON(std::string_view json);

    /**
     * @brief Create a Metadata object from an existing JSON document.
     */
    static Metadata FromJSON(rapidjson::Document json);

    private:

    /**
     * @brief Constructor is private. Use one of the static functions
     * to create a valid Metadata object.
     *
     * @param impl Pointer to implementation.
     */
    Metadata(const std::shared_ptr<MetadataImpl>& impl);

    std::shared_ptr<MetadataImpl> self;
};

}

#endif
