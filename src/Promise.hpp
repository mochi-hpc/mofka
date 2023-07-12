/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PROMISE_H
#define MOFKA_PROMISE_H

#include "mofka/Future.hpp"
#include "mofka/EventID.hpp"
#include "mofka/Exception.hpp"
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

    void setException(Exception ex) {
        auto state = m_state.lock();
        if(state)
            state->set_value(std::move(ex));
    }

    static inline std::pair<Future<Type>, Promise<Type>> CreateFutureAndPromise() {
        auto state = std::make_shared<State>();
        auto wait_fn = [state]() mutable -> EventID {
            auto v = state->wait();
            if(std::holds_alternative<Exception>(v))
                throw std::get<Exception>(v);
            return std::get<EventID>(v);
        };
        auto complete_fn = [state]() mutable -> bool {
            return state->test();
        };
        return std::make_pair(
            Future<Type>{std::move(wait_fn), std::move(complete_fn)},
            Promise<Type>{std::move(state)});
    }

    private:

    using State = thallium::eventual<std::variant<Type, Exception>>;

    Promise(std::shared_ptr<State> state)
    : m_state(std::move(state)) {}

    std::weak_ptr<State> m_state;
};

}

#endif
