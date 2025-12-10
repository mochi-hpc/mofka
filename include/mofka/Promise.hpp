/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PROMISE_H
#define MOFKA_PROMISE_H

#include <diaspora/Future.hpp>
#include <diaspora/EventID.hpp>
#include <diaspora/Exception.hpp>
#include <thallium.hpp>
#include <variant>

namespace mofka {

template<typename Type>
struct Promise {

    Promise() = default;
    Promise(const Promise&) = default;
    Promise(Promise&&) = default;
    Promise& operator=(const Promise&) = default;
    Promise& operator=(Promise&&) = default;

    void setValue(Type value) {
        auto state = m_state.lock();
        if(state)
            state->set(std::move(value));
    }

    void setException(diaspora::Exception ex) {
        auto state = m_state.lock();
        if(state)
            state->set(std::move(ex));
    }

    static inline std::pair<diaspora::Future<Type>, Promise<Type>> CreateFutureAndPromise(
        std::function<void(int)> on_wait = std::function<void(int)>{},
        std::function<void(bool)> on_test = std::function<void(bool)>{}) {
        auto state = newState();
        auto wait_fn = [state, on_wait=std::move(on_wait)](int timeout_ms) mutable -> Type {
            if(on_wait) on_wait(timeout_ms);
            return std::move(*state).wait(timeout_ms);
        };
        auto complete_fn = [state, on_test=std::move(on_test)]() mutable -> bool {
            auto is_ready = state->test();
            if(on_test) on_test(is_ready);
            return is_ready;
        };
        return std::make_pair(
            diaspora::Future<Type>{std::move(wait_fn), std::move(complete_fn)},
            Promise<Type>{std::move(state)});
    }

    private:

    struct State {

        std::atomic<bool>                       m_is_set = false;
        std::variant<Type, diaspora::Exception> m_content = Type{};
        ABT_cond_memory                         m_cv = ABT_COND_INITIALIZER;
        ABT_mutex_memory                        m_mutex = ABT_MUTEX_INITIALIZER;

        bool test() const {
            return m_is_set;
        }

        Type wait(int timeout_ms) && {
            ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&m_mutex));
            if(timeout_ms > 0) {
                struct timespec deadline, now;
                clock_gettime(CLOCK_REALTIME, &now);
                deadline = now;
                deadline.tv_nsec += timeout_ms*1000*1000;
                while(!m_is_set
                    && (now.tv_sec < deadline.tv_sec
                        || (now.tv_sec == deadline.tv_sec && now.tv_nsec < deadline.tv_nsec))) {
                    ABT_cond_timedwait(
                        ABT_COND_MEMORY_GET_HANDLE(&m_cv),
                        ABT_MUTEX_MEMORY_GET_HANDLE(&m_mutex), &deadline);
                    clock_gettime(CLOCK_REALTIME, &now);
                }
            } else {
                while(!m_is_set) {
                    ABT_cond_wait(
                        ABT_COND_MEMORY_GET_HANDLE(&m_cv),
                        ABT_MUTEX_MEMORY_GET_HANDLE(&m_mutex));
                }
            }
            if(m_content.index() == 0) {
                auto result = std::get<Type>(std::move(m_content));
                ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&m_mutex));
                return result;
            } else {
                auto ex = std::get<diaspora::Exception>(std::move(m_content));
                ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&m_mutex));
                throw ex;
            }
        }

        template<typename T>
        void set(T&& value) {
            ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&m_mutex));
            m_content = std::move(value);
            m_is_set = true;
            ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&m_mutex));
            ABT_cond_signal(ABT_COND_MEMORY_GET_HANDLE(&m_cv));
        }
    };

    static std::shared_ptr<State> newState() {
        return std::make_shared<State>();
    }

    Promise(std::shared_ptr<State> state)
    : m_state(std::move(state)) {}

    std::weak_ptr<State> m_state;
};

}

#endif
