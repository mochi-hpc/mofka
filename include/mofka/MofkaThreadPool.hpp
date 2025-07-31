/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_THREAD_POOL_H
#define MOFKA_THREAD_POOL_H

#include <diaspora/ThreadPool.hpp>
#include <thallium.hpp>
#include <vector>

namespace mofka {

class MofkaThreadPool : public diaspora::ThreadPoolInterface {

    static thallium::pool s_default_pool;

    public:

    static void SetDefaultPool(thallium::pool pool);

    static thallium::pool GetDefaultPool();

    MofkaThreadPool(diaspora::ThreadCount tc);

    MofkaThreadPool(thallium::pool pool);

    ~MofkaThreadPool();

    void pushWork(std::function<void()> func, uint64_t priority) override;

    diaspora::ThreadCount threadCount() const override {
        return diaspora::ThreadCount{m_managed_xstreams.size()};
    }

    std::size_t size() const override {
        return m_pool.total_size();
    }

    private:

    thallium::pool                                    m_pool;
    std::vector<thallium::managed<thallium::xstream>> m_managed_xstreams;
    ABT_pool_user_def                                 m_pool_def = nullptr;
};

}

#endif
