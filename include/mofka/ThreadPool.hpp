/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_THREAD_POOL_HPP
#define MOFKA_THREAD_POOL_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Exception.hpp>
#include <mofka/Future.hpp>

#include <thallium.hpp>
#include <memory>

namespace mofka {

class ActiveProducerBatchQueue;
class ThreadPoolImpl;

/**
 * @brief Strongly typped size_t meant to represent the number of
 * threads in a ThreadPool.
 */
struct ThreadCount {

    std::size_t count;

    explicit constexpr ThreadCount(std::size_t val)
    : count(val) {}
};

/**
 * @brief A ThreadPool is an object that manages a number of Argobots
 * execution streams and an Argobots pool and pushes works into them.
 * The pool holds two data structures: a FIFO queue for the ULTs that
 * have a priority set to std::numeric_limits<uint64_t>::max(), and
 * a min-heap for the ULTs that have a different priority. The pool
 * will pick alternatively from each queue.
 *
 * @warning For ULTs in the min-heap, a lower priority value means that
 * the ULT will be picked first. This is so that an EventID can be used
 * as a priority, allowing events with lower ID to be processed first.
 */
class ThreadPool {

    public:

    /**
     * @brief Constructor.
     */
    ThreadPool(ThreadCount count = ThreadCount{1});

    /**
     * @brief Copy-constructor.
     */
    ThreadPool(const ThreadPool&);

    /**
     * @brief Move-constructor.
     */
    ThreadPool(ThreadPool&&);

    /**
     * @brief Copy-assignment operator.
     */
    ThreadPool& operator=(const ThreadPool&);

    /**
     * @brief Move-assignment operator.
     */
    ThreadPool& operator=(ThreadPool&&);

    /**
     * @brief Destructor.
     */
    ~ThreadPool();

    /**
     * @brief Returns the number of underlying threads.
     */
    ThreadCount threadCount() const;

    /**
     * @brief Push work into the thread pool.
     *
     * @param func Function to push.
     * @param priority Priority.
     */
    void pushWork(std::function<void()> func,
                  uint64_t priority = std::numeric_limits<uint64_t>::max()) const;

    /**
     * @brief Get the number of ULTs in the pool, including blocked and running ULTs.
     */
    size_t size() const;

    /**
     * @brief Checks if the ThreadPool instance is valid.
     */
    explicit operator bool() const;

    private:

    /**
     * @brief Constructor is private. Use a Client object
     * to create a ThreadPool instance.
     *
     * @param impl Pointer to implementation.
     */
    ThreadPool(const std::shared_ptr<ThreadPoolImpl>& impl);

    std::shared_ptr<ThreadPoolImpl> self;

    friend class ActiveProducerBatchQueue;
};

}

#endif
