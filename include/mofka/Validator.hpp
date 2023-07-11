/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_VALIDATOR_HPP
#define MOFKA_VALIDATOR_HPP

#include <mofka/Metadata.hpp>
#include <mofka/Data.hpp>
#include <mofka/Exception.hpp>
#include <functional>
#include <exception>
#include <stdexcept>

/**
 * @brief Helper class to register validator types into the validator factory.
 */
template<typename ValidatorType>
class __MofkaValidatorRegistration;

namespace mofka {

/**
 * @brief The InvalidMetadata class is the exception raised
 * by Validators when a Metadata is not valid.
 */
class InvalidMetadata : public Exception {

    public:

    template<typename ... Args>
    InvalidMetadata(Args&&... args)
    : Exception(std::forward<Args>(args)...) {}
};

/**
 * @brief The ValidatorInterface class provides an interface for
 * validating instances of the Metadata class.
 *
 * A ValidatorInterface must also provide functions to convert
 * itself into a Metadata object an back, so that its internal
 * configuration can be stored.
 */
class ValidatorInterface {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~ValidatorInterface() = default;

    /**
     * @brief Validate that the Metadata it correct, throwing an
     * InvalidMetadata exception is the Metadata is not valid.
     * The Data associated with the Metadata is also provided,
     * although most validator are only meant to validate the
     * Metadata, not the Data content.
     *
     * @param metadata Metadata to validate.
     * @param data Associated data.
     */
    virtual void validate(const Metadata& metadata, const Data& data) const = 0;

    /**
     * @brief Convert the underlying validator implementation into a Metadata
     * object that can be stored (e.g. if the validator uses a JSON schema
     * the Metadata could contain that schema).
     */
    virtual Metadata metadata() const = 0;


    /**
     * @note A ValidatorInterface class must also provide a static Create
     * function with the following prototype, instanciating a shared_ptr of
     * the class from the provided Metadata:
     *
     * static std::shared_ptr<ValidatorInterface> Create(const Metadata&);
     */
};

class Validator {

    public:

    /**
     * @brief Constructor. Will construct a valid Validator that accepts
     * any Metadata correctly formatted in JSON.
     */
    Validator();

    /**
     * @brief Copy-constructor.
     */
    Validator(const Validator&);

    /**
     * @brief Move-constructor.
     */
    Validator(Validator&&);

    /**
     * @brief copy-assignment operator.
     */
    Validator& operator=(const Validator&);

    /**
     * @brief Move-assignment operator.
     */
    Validator& operator=(Validator&&);

    /**
     * @brief Destructor.
     */
    ~Validator();

    /**
     * @brief Checks for the validity of the underlying pointer.
     */
    operator bool() const;

    /**
     * @brief Validate that the Metadata it correct, throwing an
     * InvalidMetadata exception is the Metadata is not valid.
     * The Data associated with the Metadata is also provided,
     * although most validator are only meant to validate the
     * Metadata, not the Data content.
     *
     * @param metadata Metadata to validate.
     * @param data Associated data.
     */
    void validate(const Metadata& metadata, const Data& data) const;

    /**
     * @brief Convert the underlying validator implementation into a Metadata
     * object that can be stored (e.g. if the validator uses a JSON schema
     * the Metadata could contain that schema).
     */
    Metadata metadata() const;

    /**
     * @brief Factory function to create a Validator instance
     * when the underlying implementation is not known.
     *
     * @param metadata Metadata of the Validator.
     *
     * @return Validator instance.
     */
    static Validator FromMetadata(const Metadata& metadata);

    private:

    std::shared_ptr<ValidatorInterface> self;

    Validator(const std::shared_ptr<ValidatorInterface>& impl);

    template<typename T>
    friend class ::__MofkaValidatorRegistration;

    static void RegisterValidatorType(
            std::string_view name,
            std::function<std::shared_ptr<ValidatorInterface>(const Metadata&)> ctor);
};

}

#define MOFKA_REGISTER_VALIDATOR(__name, __type) \
    static __MofkaValidatorRegistration<__type> __mofka ## __name ## _validator( #__name )

template<typename ValidatorType>
class __MofkaValidatorRegistration {

    public:

    __MofkaValidatorRegistration(std::string_view name) {
        mofka::Validator::RegisterValidatorType(name, ValidatorType::Create);
    }
};

#endif
