/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_TARGET_SELECTOR_HPP
#define MOFKA_TARGET_SELECTOR_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/UUID.hpp>
#include <mofka/Metadata.hpp>
#include <mofka/Exception.hpp>

#include <functional>
#include <exception>
#include <stdexcept>

/**
 * @brief Helper class to register selector types into the selector factory.
 */
template<typename TargetSelectorType>
class __MofkaTargetSelectorRegistration;

namespace mofka {

class PartitionTargetInfoImpl;
class TopicHandleImpl;
class ClientImpl;
class ServiceHandle;
class ConsumerImpl;

/**
 * @brief The PartitionTargetInfo structure holds information about
 * a particular Mofka provider holding a partition of a topic. The
 * public interface allows accessing a server address, provider id,
 * and a UUID. Contrary to the address and provider id, the UUID
 * is meant not to change when the service is restarted or when
 * the partition migrates to another server.
 */
class PartitionTargetInfo {

    public:

    /**
     * @brief Identifier uniquely identifying the partition even if
     * the service restarts or the partition is migrated to another server.
     */
    UUID uuid() const;

    /**
     * @brief Current address of the target.
     */
    const std::string& address() const;

    /**
     * @brief Current provider ID.
     */
    uint16_t providerID() const;

    /**
     * @brief Move constructor.
     */
    PartitionTargetInfo(PartitionTargetInfo&&);

    /**
     * @brief Copy constructor.
     */
    PartitionTargetInfo(const PartitionTargetInfo&);

    /**
     * @brief Move-assignment operator.
     */
    PartitionTargetInfo& operator=(PartitionTargetInfo&&);

    /**
     * @brief Copy-assignment operator.
     */
    PartitionTargetInfo& operator=(const PartitionTargetInfo&);

    /**
     * @brief Destructor.
     */
    ~PartitionTargetInfo();

    /**
     * @brief Checks for the validity of the internal self pointer.
     */
    operator bool() const;

    private:

    std::shared_ptr<PartitionTargetInfoImpl> self;

    PartitionTargetInfo(const std::shared_ptr<PartitionTargetInfoImpl>& impl);

    friend class std::hash<PartitionTargetInfo>;
    friend class TopicHandleImpl;
    friend class ClientImpl;
    friend class Producer;
    friend class ServiceHandle;
    friend class ConsumerImpl;
    friend class Event;
};

/**
 * @brief The TargetSelectorInterface class provides an interface for
 * objects that decide how which target server will receive each
 * event.
 *
 * A TargetSelectorInterface must also provide functions to convert
 * itself into a Metadata object an back, so that its internal
 * configuration can be stored.
 */
class TargetSelectorInterface {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~TargetSelectorInterface() = default;

    /**
     * @brief Sets the list of targets that are available to store events.
     *
     * @param targets Vector of PartitionTargetInfo.
     */
    virtual void setTargets(const std::vector<PartitionTargetInfo>& targets) = 0;

    /**
     * @brief Selects a partition target to use to store the given event.
     *
     * @param metadata Metadata of the event.
     */
    virtual PartitionTargetInfo selectTargetFor(const Metadata& metadata) = 0;

    /**
     * @brief Convert the underlying validator implementation into a Metadata
     * object that can be stored (e.g. if the validator uses a JSON schema
     * the Metadata could contain that schema).
     */
    virtual Metadata metadata() const = 0;

    /**
     * @note A TargetSelectorInterface class must also provide a static Create
     * function with the following prototype, instanciating a shared_ptr of
     * the class from the provided Metadata:
     *
     * static std::shared_ptr<TargetSelectorInterface> Create(const Metadata&);
     */
};

class TargetSelector {

    public:

    /**
     * @brief Constructor. Will construct a valid TargetSelector that accepts
     * any Metadata correctly formatted in JSON.
     */
    TargetSelector();

    /**
     * @brief Copy-constructor.
     */
    TargetSelector(const TargetSelector&);

    /**
     * @brief Move-constructor.
     */
    TargetSelector(TargetSelector&&);

    /**
     * @brief copy-assignment operator.
     */
    TargetSelector& operator=(const TargetSelector&);

    /**
     * @brief Move-assignment operator.
     */
    TargetSelector& operator=(TargetSelector&&);

    /**
     * @brief Destructor.
     */
    ~TargetSelector();

    /**
     * @brief Checks for the validity of the underlying pointer.
     */
    operator bool() const;

    /**
     * @brief Sets the list of targets that are available to store events.
     *
     * @param targets Vector of PartitionTargetInfo.
     */
    void setTargets(const std::vector<PartitionTargetInfo>& targets);

    /**
     * @brief Selects a partition target to use to store the given event.
     *
     * @param metadata Metadata of the event.
     */
    PartitionTargetInfo selectTargetFor(const Metadata& metadata);

    /**
     * @brief Convert the underlying validator implementation into a Metadata
     * object that can be stored (e.g. if the validator uses a JSON schema
     * the Metadata could contain that schema).
     */
    Metadata metadata() const;

    /**
     * @brief Factory function to create a TargetSelector instance
     * when the underlying implementation is not known.
     *
     * @param metadata Metadata of the TargetSelector.
     *
     * @return TargetSelector instance.
     */
    static TargetSelector FromMetadata(const Metadata& metadata);

    private:

    std::shared_ptr<TargetSelectorInterface> self;

    TargetSelector(const std::shared_ptr<TargetSelectorInterface>& impl);

    template<typename T>
    friend class ::__MofkaTargetSelectorRegistration;

    static void RegisterTargetSelectorType(
            std::string_view name,
            std::function<std::shared_ptr<TargetSelectorInterface>(const Metadata&)> ctor);
};

}

#define MOFKA_REGISTER_TARGET_SELECTOR(__name, __type) \
    static __MofkaTargetSelectorRegistration<__type> __mofka ## __name ## _validator( #__name )

template<typename TargetSelectorType>
class __MofkaTargetSelectorRegistration {

    public:

    __MofkaTargetSelectorRegistration(std::string_view name) {
        mofka::TargetSelector::RegisterTargetSelectorType(name, TargetSelectorType::Create);
    }
};

#endif
