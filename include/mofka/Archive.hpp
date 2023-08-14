/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_ARCHIVE_HPP
#define MOFKA_ARCHIVE_HPP

#include <mofka/ForwardDcl.hpp>

#include <cstdint>
#include <functional>

namespace mofka {

/**
 * @brief The Archive class is an interface used by the serialization
 * functionalities inside Mofka. The user does not have to implement
 * an archive, however this interface may be used when defining custom
 * Serializer implementations.
 */
class Archive {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~Archive() = default;

    /**
     * @brief Read size bytes from the archive into the buffer.
     *
     * @param buffer Buffer.
     * @param size Number of bytes to read.
     */
    virtual void read(void* buffer, std::size_t size) = 0;

    /**
     * @brief Write size bytes from the buffer into the archive.
     *
     * @param buffer Buffer.
     * @param size Number of bytes to write.
     */
    virtual void write(const void* buffer, size_t size) = 0;

};

}

#endif
