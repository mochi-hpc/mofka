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
            state->set_value(std::move(value));
    }

    void setException(diaspora::Exception ex) {
        auto state = m_state.lock();
        if(state)
            state->set_value(std::move(ex));
    }

    static inline std::pair<diaspora::Future<Type>, Promise<Type>> CreateFutureAndPromise(
        std::function<void()> on_wait = std::function<void()>{},
        std::function<void(bool)> on_test = std::function<void(bool)>{}) {
        auto state = newState();
        auto wait_fn = [state, on_wait=std::move(on_wait)]() mutable -> Type {
            if(on_wait) on_wait();
            auto v = std::move(*state).wait();
            if(std::holds_alternative<diaspora::Exception>(v))
                throw std::get<diaspora::Exception>(v);
            return std::get<Type>(std::move(v));
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

        std::variant<Type, diaspora::Exception> m_content;
        ABT_eventual_memory                     m_eventual = ABT_EVENTUAL_INITIALIZER;

        bool test() const {
            ABT_bool flag;
            ABT_eventual_test(ABT_EVENTUAL_MEMORY_GET_HANDLE(&m_eventual), nullptr, &flag);
            return flag;
        }

        std::variant<Type, diaspora::Exception>&& wait() && {
            ABT_eventual_wait(ABT_EVENTUAL_MEMORY_GET_HANDLE(&m_eventual), nullptr);
            return std::move(m_content);
        }

        template<typename T>
        void set_value(T&& value) {
            m_content = std::move(value);
            ABT_eventual_set(ABT_EVENTUAL_MEMORY_GET_HANDLE(&m_eventual), nullptr, 0);
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
