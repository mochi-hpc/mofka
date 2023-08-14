/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_BUFFER_WRAPPER_ARCHIVE_HPP
#define MOFKA_BUFFER_WRAPPER_ARCHIVE_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Archive.hpp>
#include <mofka/Exception.hpp>

#include <cstring>
#include <string_view>

namespace mofka {

struct BufferWrapperOutputArchive : public Archive {

    void read(void* buffer, std::size_t size) override {
        /* this function is not supposed to be called */
        (void)buffer;
        (void)size;
    }

    void write(const void* data, size_t size) override {
        auto new_size = m_buffer.size() + size;
        if(m_buffer.capacity() < m_buffer.size() + size) {
            m_buffer.reserve(2*new_size);
        }
        auto offset = m_buffer.size();
        m_buffer.resize(new_size);
        std::memcpy(m_buffer.data() + offset, data, size);
    }

    BufferWrapperOutputArchive(std::vector<char>& buf)
    : m_buffer(buf) {}

    std::vector<char>& m_buffer;
};

struct BufferWrapperInputArchive : public Archive {

    void read(void* buffer, std::size_t size) override {
        if(size > m_buffer.size())
            throw Exception(
                    "BufferWrapperInputArchive error: trying to read more than the buffer size");
        std::memcpy(buffer, m_buffer.data(), size);
        m_buffer = std::string_view{m_buffer.data() + size, m_buffer.size() - size};
    }

    void write(const void* data, size_t size) override {
        (void)data;
        (void)size;
        /* this function is not supposed to be used */
    }

    BufferWrapperInputArchive(std::string_view buf)
    : m_buffer(buf) {}

    std::string_view m_buffer;
};

}

#endif
