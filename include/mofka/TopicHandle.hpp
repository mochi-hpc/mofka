/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_TOPIC_HANDLE_HPP
#define MOFKA_TOPIC_HANDLE_HPP

#include <thallium.hpp>
#include <rapidjson/document.h>
#include <mofka/Client.hpp>
#include <mofka/Exception.hpp>
#include <mofka/AsyncRequest.hpp>
#include <memory>
#include <unordered_set>

namespace mofka {

class ServiceHandle;
class TopicHandleImpl;

/**
 * @brief A TopicHandle object is a handle for a remote topic
 * on a set of servers. It enables invoking the topic's functionalities.
 */
class TopicHandle {

    friend class ServiceHandle;

    public:

    /**
     * @brief Constructor. The resulting TopicHandle handle will be invalid.
     */
    TopicHandle();

    /**
     * @brief Copy-constructor.
     */
    TopicHandle(const TopicHandle&);

    /**
     * @brief Move-constructor.
     */
    TopicHandle(TopicHandle&&);

    /**
     * @brief Copy-assignment operator.
     */
    TopicHandle& operator=(const TopicHandle&);

    /**
     * @brief Move-assignment operator.
     */
    TopicHandle& operator=(TopicHandle&&);

    /**
     * @brief Destructor.
     */
    ~TopicHandle();

    /**
     * @brief Returns the ServiceHandle this topic has been opened with.
     */
    ServiceHandle service() const;


    /**
     * @brief Checks if the TopicHandle instance is valid.
     */
    operator bool() const;

    /**
     * @brief Sends an RPC to the topic to make it print a hello message.
     */
    void sayHello() const;

    /**
     * @brief Requests the target topic to compute the sum of two numbers.
     * If result is null, it will be ignored. If req is not null, this call
     * will be non-blocking and the caller is responsible for waiting on
     * the request.
     *
     * @param[in] x first integer
     * @param[in] y second integer
     * @param[out] result result
     * @param[out] req request for a non-blocking operation
     */
    void computeSum(int32_t x, int32_t y,
                    int32_t* result = nullptr,
                    AsyncRequest* req = nullptr) const;

    private:

    /**
     * @brief Constructor is private. Use a Client object
     * to create a TopicHandle instance.
     *
     * @param impl Pointer to implementation.
     */
    TopicHandle(const std::shared_ptr<TopicHandleImpl>& impl);

    std::shared_ptr<TopicHandleImpl> self;
};

}

#endif
