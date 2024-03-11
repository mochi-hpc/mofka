/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_SERIALIZER_HPP
#define MOFKA_SERIALIZER_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Archive.hpp>
#include <mofka/Metadata.hpp>
#include <mofka/InvalidMetadata.hpp>
#include <mofka/Factory.hpp>

#include <functional>
#include <exception>
#include <stdexcept>

namespace mofka {

/**
 * @brief The SerializerInterface class provides an interface for
 * serializing the Metadata class. Its serialize and deserialize
 * methods should make use of the Archive's write and read methods
 * respectively.
 *
 * A SerializerInterface must also provide functions to convert
 * itself into a Metadata object an back, so that its internal
 * configuration can be stored.
 */
class SerializerInterface {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~SerializerInterface() = default;

    /**
     * @brief Serialize the Metadata into the Archive.
     * Errors whould be handled by throwing a mofka::Exception.
     *
     * @param archive Archive into which to serialize the metadata.
     * @param metadata Metadata to serialize.
     */
    virtual void serialize(Archive& archive, const Metadata& metadata) const = 0;

    /**
     * @brief Deserialize the Metadata from the Archive.
     * Errors whould be handled by throwing a mofka::Exception.
     *
     * @param archive Archive from which to deserialize the metadata.
     * @param metadata Metadata to deserialize.
     */
    virtual void deserialize(Archive& archive, Metadata& metadata) const = 0;

    /**
     * @brief Convert the underlying serializer implementation into a Metadata
     * object that can be stored (e.g. if the serializer uses a compression
     * algorithm, the Metadata could contain the parameters used for
     * compression, so that someone could restore a Serializer with the
     * same parameters later when deserializing objects).
     *
     * @param metadata Metadata representing the internals of the serializer.
     */
    virtual Metadata metadata() const = 0;

    /**
     * @note A SerializerInterface class must also provide a static create
     * function with the following prototype, instanciating a shared_ptr of
     * the class from the provided Metadata:
     *
     * static std::unique_ptr<SerializerInterface> create(const Metadata&);
     */
};

class Serializer {

    public:

    /**
     * @brief Constructor. Will construct a valid Serializer that simply
     * serializes the Metadata into JSON string.
     */
    Serializer();

    /**
     * @brief Copy-constructor.
     */
    Serializer(const Serializer&);

    /**
     * @brief Move-constructor.
     */
    Serializer(Serializer&&);

    /**
     * @brief copy-assignment operator.
     */
    Serializer& operator=(const Serializer&);

    /**
     * @brief Move-assignment operator.
     */
    Serializer& operator=(Serializer&&);

    /**
     * @brief Destructor.
     */
    ~Serializer();

    /**
     * @brief Serialize the Metadata into the Archive.
     * Errors whould be handled by throwing a mofka::Exception.
     *
     * @param archive Archive into which to serialize the metadata.
     * @param metadata Metadata to serialize.
     */
    void serialize(Archive& archive, const Metadata& metadata) const;

    /**
     * @brief Deserialize the Metadata from the Archive.
     * Errors whould be handled by throwing a mofka::Exception.
     *
     * @param archive Archive from which to deserialize the metadata.
     * @param metadata Metadata to deserialize.
     */
    void deserialize(Archive& archive, Metadata& metadata) const;

    /**
     * @brief Convert the underlying serializer implementation into a Metadata
     * object that can be stored (e.g. if the serializer uses a compression
     * algorithm, the Metadata could contain the parameters used for
     * compression, so that someone could restore a Serializer with the
     * same parameters later when deserializing objects).
     *
     * @param metadata Metadata representing the internals of the serializer.
     */
    Metadata metadata() const;

    /**
     * @brief Factory function to create a Serializer instance.
     *
     * @param type Type of Serializer.
     * @param metadata Metadata of the Serializer.
     *
     * @return Serializer instance.
     */
    static Serializer FromMetadata(const char* type, const Metadata& metadata);

    /**
     * @brief Same as the above function but the type is expected
     * to be provided as a "__type__" field in the metdata, and the
     * function will fall back to "default" if not provided.
     *
     * @param metadata Metadata of the Serializer.
     *
     * @return Serializer instance.
     */
    static Serializer FromMetadata(const Metadata& metadata);

    /**
     * @brief Checks for the validity of the underlying pointer.
     */
    operator bool() const;

    private:

    std::shared_ptr<SerializerInterface> self;

    Serializer(const std::shared_ptr<SerializerInterface>& impl);

};

using SerializerFactory = Factory<SerializerInterface, const Metadata&>;

}


#define MOFKA_REGISTER_SERIALIZER(__name__, __type__) \
    MOFKA_REGISTER_IMPLEMENTATION_FOR(SerializerFactory, __type__, __name__)

#endif
