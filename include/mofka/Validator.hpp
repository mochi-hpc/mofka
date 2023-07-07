/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_VALIDATOR_HPP
#define MOFKA_VALIDATOR_HPP

#include <mofka/Metadata.hpp>
#include <mofka/Exception.hpp>
#include <exception>
#include <stdexcept>

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
     *
     * @param metadata Metadata to validate.
     */
    virtual void validate(const Metadata& metadata) const = 0;

    /**
     * @brief Convert the underlying validator implementation into a Metadata
     * object that can be stored (e.g. if the validator uses a JSON schema
     * the Metadata could contain that schema).
     */
    virtual Metadata metadata() const = 0;

    /**
     * @brief Restore the state of a validator from the provided Metadata.
     *
     * @param metadata Metadata containing the state of the validator.
     */
    virtual void fromMetadata(const Metadata& metadata) = 0;
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
     *
     * @param metadata Metadata to validate.
     */
    void validate(const Metadata& metadata) const;

    /**
     * @brief Convert the underlying validator implementation into a Metadata
     * object that can be stored (e.g. if the validator uses a JSON schema
     * the Metadata could contain that schema).
     */
    Metadata metadata() const;

    /**
     * @brief Restore the state of a validator from the provided Metadata.
     *
     * @param metadata Metadata containing the state of the validator.
     */
    void fromMetadata(const Metadata& metadata);

    private:

    std::shared_ptr<ValidatorInterface> self;

    Validator(const std::shared_ptr<ValidatorInterface>& impl);
};

}

#endif
