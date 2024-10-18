/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PARTITION_SELECTOR_HPP
#define MOFKA_PARTITION_SELECTOR_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Metadata.hpp>
#include <mofka/Exception.hpp>
#include <mofka/InvalidMetadata.hpp>
#include <mofka/Factory.hpp>

#include <functional>
#include <exception>
#include <stdexcept>

namespace mofka {

/**
 * @brief The PartitionInfo structure holds information about a particular partition.
 * The information contained in such a Metadata piece is implementation-defined.
 */
class PartitionInfo : public Metadata {

};

/**
 * @brief The PartitionSelectorInterface class provides an interface for
 * objects that decide how which target server will receive each
 * event.
 *
 * A PartitionSelectorInterface must also provide functions to convert
 * itself into a Metadata object an back, so that its internal
 * configuration can be stored.
 */
class PartitionSelectorInterface {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~PartitionSelectorInterface() = default;

    /**
     * @brief Sets the list of targets that are available to store events.
     *
     * @param targets Vector of PartitionInfo.
     */
    virtual void setPartitions(const std::vector<PartitionInfo>& targets) = 0;

    /**
     * @brief Selects a partition target to use to store the given event.
     * The returned value should be an index between 0 and N-1 where N is the
     * size of the array passed to setPartitions().
     *
     * @param metadata Metadata of the event.
     */
    virtual size_t selectPartitionFor(const Metadata& metadata) = 0;

    /**
     * @brief Convert the underlying validator implementation into a Metadata
     * object that can be stored (e.g. if the validator uses a JSON schema
     * the Metadata could contain that schema).
     */
    virtual Metadata metadata() const = 0;

    /**
     * @note A PartitionSelectorInterface class must also provide a static create
     * function with the following prototype, instanciating a shared_ptr of
     * the class from the provided Metadata:
     *
     * static std::shared_ptr<PartitionSelectorInterface> create(const Metadata&);
     */
};

class PartitionSelector {

    public:

    /**
     * @brief Constructor. Will construct a valid PartitionSelector that accepts
     * any Metadata correctly formatted in JSON.
     */
    PartitionSelector();

    /**
     * @brief Copy-constructor.
     */
    PartitionSelector(const PartitionSelector&);

    /**
     * @brief Move-constructor.
     */
    PartitionSelector(PartitionSelector&&);

    /**
     * @brief copy-assignment operator.
     */
    PartitionSelector& operator=(const PartitionSelector&);

    /**
     * @brief Move-assignment operator.
     */
    PartitionSelector& operator=(PartitionSelector&&);

    /**
     * @brief Destructor.
     */
    ~PartitionSelector();

    /**
     * @brief Checks for the validity of the underlying pointer.
     */
    operator bool() const;

    /**
     * @brief Sets the list of targets that are available to store events.
     *
     * @param targets Vector of PartitionInfo.
     */
    void setPartitions(const std::vector<PartitionInfo>& targets);

    /**
     * @brief Selects a partition target to use to store the given event.
     *
     * @param metadata Metadata of the event.
     */
    size_t selectPartitionFor(const Metadata& metadata);

    /**
     * @brief Convert the underlying validator implementation into a Metadata
     * object that can be stored (e.g. if the validator uses a JSON schema
     * the Metadata could contain that schema).
     */
    Metadata metadata() const;

    /**
     * @brief Factory function to create a PartitionSelector instance
     * from its type and configuration.
     *
     * @param type Type of PartitionSelector.
     * @param metadata Metadata of the PartitionSelector.
     *
     * @return PartitionSelector instance.
     */
    static PartitionSelector FromMetadata(const char* type, const Metadata& metadata);

    /**
     * @brief Version of the above function that does not require a type.
     * The type will be obtained from a "__type__" field in the metadata,
     * and will fall back to "default" if not provided.
     *
     * @param metadata Metadata of the PartitionSelector.
     *
     * @return PartitionSelector instance.
     */
    static PartitionSelector FromMetadata(const Metadata& metadata);

    private:

    std::shared_ptr<PartitionSelectorInterface> self;

    PartitionSelector(const std::shared_ptr<PartitionSelectorInterface>& impl);
};

using PartitionSelectorFactory = Factory<PartitionSelectorInterface, const Metadata&>;

#define MOFKA_REGISTER_PARTITION_SELECTOR(__name__, __type__) \
    MOFKA_REGISTER_IMPLEMENTATION_FOR(PartitionSelectorFactory, __type__, __name__)

}

#endif
