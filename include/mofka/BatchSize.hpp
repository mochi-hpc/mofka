/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_BATCH_SIZE_HPP
#define MOFKA_BATCH_SIZE_HPP

#include <cstdint>

namespace mofka {

/**
 * @brief Strongly typped size_t meant to store the batch size to
 * use when creating a Producer.
 */
struct BatchSize {

    std::size_t value;

    explicit constexpr BatchSize(std::size_t val)
    : value(val) {}

    /**
     * @brief Returns a value telling the producer to try its best
     * to adapt the batch size to the use-case and workload.
     */
    static BatchSize Adaptive();

    inline bool operator<(const BatchSize& other) const { return value < other.value; }
    inline bool operator>(const BatchSize& other) const { return value > other.value; }
    inline bool operator<=(const BatchSize& other) const { return value <= other.value; }
    inline bool operator>=(const BatchSize& other) const { return value >= other.value; }
    inline bool operator==(const BatchSize& other) const { return value == other.value; }
    inline bool operator!=(const BatchSize& other) const { return value != other.value; }
};

}

#endif
