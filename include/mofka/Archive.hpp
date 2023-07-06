/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_ARCHIVE_HPP
#define MOFKA_ARCHIVE_HPP

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
     * @brief Feed is a function of read that feeds the underlying
     * raw data into a user-provided function. The function will be
     * called only once, on a memory segment representing the whole
     * content of the archive.
     *
     * @param reader Reader function.
     */
    virtual void feed(const std::function<void(const void*, std::size_t)>& reader) = 0;

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
