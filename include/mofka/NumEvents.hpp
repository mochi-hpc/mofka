/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_NUM_EVENTS_HPP
#define MOFKA_NUM_EVENTS_HPP

#include <cstdint>

namespace mofka {

/**
 * @brief Strongly typped size_t meant to store the batch size to
 * use when creating a Producer.
 */
struct NumEvents {

    std::size_t value;

    explicit constexpr NumEvents(std::size_t val)
    : value(val) {}

    /**
     * @brief A value so large you are unlikely to see than many events
     * in the lifetime of the application.
     */
    static NumEvents Infinity();

    inline bool operator<(const NumEvents& other) const { return value < other.value; }
    inline bool operator>(const NumEvents& other) const { return value > other.value; }
    inline bool operator<=(const NumEvents& other) const { return value <= other.value; }
    inline bool operator>=(const NumEvents& other) const { return value >= other.value; }
    inline bool operator==(const NumEvents& other) const { return value == other.value; }
    inline bool operator!=(const NumEvents& other) const { return value != other.value; }
};

}

#endif
