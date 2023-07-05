/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PIMPL_UTIL_HPP
#define MOFKA_PIMPL_UTIL_HPP

#define PIMPL_DEFINE_COMMON_FUNCTIONS(T)     \
    T::T() = default;                        \
    T::T(const std::shared_ptr<T ## Impl>& impl) \
    : self(impl) {}                          \
    T::T(const T&) = default;                \
    T::T(T&&) = default;                     \
    T& T::operator=(const T&) = default;     \
    T& T::operator=(T&&) = default;          \
    T::operator bool() const {               \
        return static_cast<bool>(self);      \
    }                                        \
    T::~T() = default

#endif
