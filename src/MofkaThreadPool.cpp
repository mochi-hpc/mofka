/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <diaspora/Exception.hpp>

#include "mofka/MofkaThreadPool.hpp"

#include "PrioPool.hpp"

namespace mofka {

thallium::pool MofkaThreadPool::s_default_pool = thallium::pool{};

void MofkaThreadPool::SetDefaultPool(thallium::pool pool) {
    s_default_pool = pool;
}

thallium::pool MofkaThreadPool::GetDefaultPool() {
    if(s_default_pool.is_null())
        s_default_pool = thallium::xstream::self().get_main_pools(1)[0];
    return s_default_pool;
}

MofkaThreadPool::MofkaThreadPool(diaspora::ThreadCount tc) {
    if(tc.count == 0) {
        m_pool = GetDefaultPool();
    } else {
        ABT_pool_prio_wait_def_create(&m_pool_def);
        ABT_pool_config pool_config = ABT_POOL_CONFIG_NULL;
        ABT_pool pool = ABT_POOL_NULL;
        ABT_pool_create(m_pool_def, pool_config, &pool);
        m_pool = thallium::pool{pool};
        for(std::size_t i=0; i < tc.count; ++i) {
            auto sched = thallium::scheduler::predef::basic_wait;
            m_managed_xstreams.push_back(thallium::xstream::create(sched, m_pool));
        }
    }
}

MofkaThreadPool::MofkaThreadPool(thallium::pool pool)
: m_pool{pool} {}

MofkaThreadPool::~MofkaThreadPool() {
    for(auto& x : m_managed_xstreams) {
        x->join();
    }
    m_managed_xstreams.clear();
    if(m_pool_def) {
        ABT_pool_user_def_free(&m_pool_def);
        ABT_pool pool = m_pool.native_handle();
        ABT_pool_free(&pool);
    }
}

void MofkaThreadPool::pushWork(std::function<void()> func, uint64_t priority) {
    if(!m_pool_def) { // not custom priority pool
        m_pool.make_thread(std::move(func), thallium::anonymous{});
        thallium::thread::yield();
    } else { // custom priority pool, first argument should be a priority
        struct Args {
            uint64_t              priority;
            std::function<void()> func;
        };
        auto func_wrapper = [](void* args) {
            auto a = static_cast<Args*>(args);
            a->func();
            delete a;
        };
        auto args = new Args{priority, std::move(func)};
        ABT_thread_create(m_pool.native_handle(),
                func_wrapper, args,
                ABT_THREAD_ATTR_NULL, nullptr);

    }
}

}
