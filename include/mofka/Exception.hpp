/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_EXCEPTION_HPP
#define MOFKA_EXCEPTION_HPP

#include <fmt/format.h>
#include <exception>

namespace mofka {

class Exception : public std::exception {

    std::string m_error;

    public:

    template<typename ... Args>
    Exception(Args&&... args)
    : m_error(fmt::format(std::forward<Args>(args)...)) {}

    virtual const char* what() const noexcept override {
        return m_error.c_str();
    }
};

}

#endif
