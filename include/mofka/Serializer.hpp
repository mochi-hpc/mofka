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

#include <functional>
#include <exception>
#include <stdexcept>

/**
 * @brief Helper class to register Serializer types into the Serializer factory.
 */
template<typename SerializerType>
class __MofkaSerializerRegistration;

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
     * @note A SerializerInterface class must also provide a static Create
     * function with the following prototype, instanciating a shared_ptr of
     * the class from the provided Metadata:
     *
     * static std::shared_ptr<SerializerInterface> Create(const Metadata&);
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
     * @brief Factory function to create a Serializer instance
     * when the underlying implementation is not known.
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

    template<typename T>
    friend class ::__MofkaSerializerRegistration;

    static void RegisterSerializerType(
            std::string_view name,
            std::function<std::shared_ptr<SerializerInterface>(const Metadata&)> ctor);
};

}

#define MOFKA_REGISTER_SERIALIZER(__name, __type) \
    static __MofkaSerializerRegistration<__type> __mofka ## __name ## _serializer( #__name )

template<typename SerializerType>
class __MofkaSerializerRegistration {

    public:

    __MofkaSerializerRegistration(std::string_view name) {
        mofka::Serializer::RegisterSerializerType(name, SerializerType::Create);
    }
};

#endif
