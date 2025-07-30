/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_CEREAL_ARCHIVE_ADAPTOR_H
#define MOFKA_CEREAL_ARCHIVE_ADAPTOR_H

#include <diaspora/Archive.hpp>

#include <functional>

namespace mofka {

template<typename A>
struct CerealInpuArchiveAdaptor : public diaspora::Archive {

    std::reference_wrapper<A> m_archive;

    CerealInpuArchiveAdaptor(std::reference_wrapper<A> a)
    : m_archive(a) {}

    void read(void* buffer, std::size_t size) override {
        m_archive.get().read((char*)buffer, size);
    }

    void write(const void* buffer, size_t size) override {
        (void)buffer;
        (void)size;
    }

};

template<typename A>
struct CerealOutpuArchiveAdaptor : public diaspora::Archive {

    std::reference_wrapper<A> m_archive;

    CerealOutpuArchiveAdaptor(std::reference_wrapper<A> a)
    : m_archive(a) {}

    void read(void* buffer, std::size_t size) override {
        (void)buffer;
        (void)size;
    }

    void write(const void* buffer, size_t size) override {
        m_archive.get().write((const char*)buffer, size);
    }

};

template<typename T>
struct Cerealized {

    T content;

    template<typename ... Args>
    Cerealized(Args&&... args)
    : content(std::forward<Args>(args)...) {}

    template<typename A>
    void save(A& ar) const {
        auto wrapper = CerealOutpuArchiveAdaptor<A>(ar);
        content.save(wrapper);
    }

    template<typename A>
    void load(A& ar) {
        auto wrapper = CerealInpuArchiveAdaptor<A>(ar);
        content.load(wrapper);
    }
};

}

#endif
