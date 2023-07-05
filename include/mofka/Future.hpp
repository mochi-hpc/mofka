/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_FUTURE_HPP
#define MOFKA_FUTURE_HPP

#include <mofka/Exception.hpp>
#include <memory>
#include <functional>

namespace mofka {

class FutureImpl;

/**
 * @brief Future objects are used to keep track of
 * on-going asynchronous operations.
 */
template<typename ResultType>
class Future {

    public:

    /**
     * @brief Default constructor. Will create a non-valid Future.
     */
    Future() = default;

    /**
     * @brief Copy constructor.
     */
    Future(const Future& other) = default;

    /**
     * @brief Move constructor.
     */
    Future(Future&& other) = default;

    /**
     * @brief Copy-assignment operator.
     */
    Future& operator=(const Future& other) = default;

    /**
     * @brief Move-assignment operator.
     */
    Future& operator=(Future&& other) = default;

    /**
     * @brief Destructor.
     */
    ~Future() = default;

    /**
     * @brief Wait for the request to complete.
     */
    ResultType wait() const {
        if(!m_wait)
            throw Exception("Calling Future::wait on an invalid future");
        return m_wait(self);
    }

    /**
     * @brief Test if the request has completed, without blocking.
     */
    bool completed() const {
        if(!m_completed)
            throw Exception("Calling Future::completed on an invalid future");
        return m_completed(self);
    }

    /**
     * @brief Checks if the Collection object is valid.
     */
    operator bool() const {
        return static_cast<bool>(self);
    }

    /**
     * @brief Constructor meant for classes that actually know what
     * a FutureImpl is.
     */
    Future(const std::shared_ptr<FutureImpl>& impl,
           std::function<ResultType(std::shared_ptr<FutureImpl>)> wait_fn,
           std::function<bool(std::shared_ptr<FutureImpl>)> completed_fn)
    : self(impl)
    , m_wait(std::move(wait_fn))
    , m_completed(std::move(completed_fn)) {}

    private:

    std::shared_ptr<FutureImpl>                            self;
    std::function<ResultType(std::shared_ptr<FutureImpl>)> m_wait;
    std::function<bool(std::shared_ptr<FutureImpl>)>       m_completed;

};

}

#endif
