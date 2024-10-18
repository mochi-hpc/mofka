/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PIMPL_UTIL_HPP
#define MOFKA_PIMPL_UTIL_HPP

#include <memory>

#define PIMPL_DEFINE_COMMON_FUNCTIONS_NO_DCTOR(T) \
    T::T(const std::shared_ptr<T ## Impl>& impl)  \
    : self(impl) {}                               \
    T::T(const T&) = default;                     \
    T::T(T&&) = default;                          \
    T& T::operator=(const T&) = default;          \
    T& T::operator=(T&&) = default;               \
    T::operator bool() const {                    \
        return static_cast<bool>(self);           \
    }

#define PIMPL_DEFINE_COMMON_FUNCTIONS_NO_CTOR(T) \
    PIMPL_DEFINE_COMMON_FUNCTIONS_NO_DCTOR(T)    \
    T::~T() = default

#define PIMPL_DEFINE_COMMON_FUNCTIONS_NO_DTOR(T) \
    T::T() = default;                            \
    PIMPL_DEFINE_COMMON_FUNCTIONS_NO_DCTOR(T)

#define PIMPL_DEFINE_COMMON_FUNCTIONS(T)     \
    T::T() = default;                        \
    PIMPL_DEFINE_COMMON_FUNCTIONS_NO_CTOR(T)

namespace mofka {

template<typename T>
using SP = std::shared_ptr<T>;

template<typename T>
using WP = std::weak_ptr<T>;

}

#endif
