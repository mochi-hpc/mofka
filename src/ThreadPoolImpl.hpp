/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_THREAD_POOL_IMPL_H
#define MOFKA_THREAD_POOL_IMPL_H

#include "mofka/ThreadPool.hpp"
#include "PrioPool.hpp"
#include <thallium.hpp>
#include <vector>

namespace mofka {

class ThreadPoolImpl {

    public:

    ThreadPoolImpl(ThreadCount tc) {
        if(tc.count == 0) {
            m_pool = thallium::xstream::self().get_main_pools(1)[0];
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

    ~ThreadPoolImpl() {
        for(auto& x : m_managed_xstreams) {
            x->join();
        }
        if(m_pool_def)
            ABT_pool_user_def_free(&m_pool_def);
    }

    template<typename Function>
    void pushWork(Function&& func, uint64_t priority=0) {
        if(!m_pool_def) { // not custom priority pool
            m_pool.make_thread(std::forward<Function>(func), thallium::anonymous{});
            thallium::thread::yield();
        } else { // custom priority pool, first argument should be a priority
            struct Args {
                uint64_t priority;
                uint64_t cs_count;
                Function func;
            };
            auto func_wrapper = [](void* args) {
                auto a = static_cast<Args*>(args);
                a->func();
                delete a;
            };
            auto args = new Args{priority, 0, std::forward<Function>(func)};
            ABT_thread_create(m_pool.native_handle(),
                              func_wrapper, args,
                              ABT_THREAD_ATTR_NULL, nullptr);

        }
    }

    std::size_t managed_xstreams_size() const {
        return m_managed_xstreams.size();
    }

    std::size_t size() const {
        return m_pool.total_size();
    }

    private:

    thallium::pool                                    m_pool;
    std::vector<thallium::managed<thallium::xstream>> m_managed_xstreams;
    ABT_pool_user_def                                 m_pool_def = nullptr;
};

}

#endif
