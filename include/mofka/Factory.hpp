/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_FACTORY_HPP
#define MOFKA_FACTORY_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Exception.hpp>
#include <dlfcn.h>
#include <unordered_map>
#include <functional>
#include <memory>
#include <vector>

namespace mofka {

template <typename Base, typename... Args>
class Factory;

template <typename FactoryType, typename Derived>
struct Registrar;

template <typename Base, typename... Args>
class Factory {

    public:

    static std::unique_ptr<Base> create(const std::string& key, Args&&... args) {
        auto& factory = instance();
        std::string name = key;
        std::size_t found = key.find(":");
        if (found != std::string::npos) {
            name = key.substr(0, found);
            const auto path = key.substr(found + 1);
            auto it = factory.m_creator_fn.find(name);
            if (it == factory.m_creator_fn.end()) {
                dlopen(path.c_str(), RTLD_NOW);
            }
        }
        auto it = factory.m_creator_fn.find(name);
        if (it != factory.m_creator_fn.end()) {
            return it->second(std::forward<Args>(args)...);
        } else {
            throw Exception("Creator not found");
        }
    }

private:

    template <typename FactoryType, typename Derived>
    friend struct Registrar;

    using CreatorFunction = std::function<std::unique_ptr<Base>(Args...)>;

    static Factory& instance() {
        static Factory factory;
        return factory;
    }

    void registerCreator(const std::string& key, CreatorFunction creator) {
        m_creator_fn[key] = std::move(creator);
    }

    std::unordered_map<std::string, CreatorFunction> m_creator_fn;
};

template <typename FactoryType, typename Derived>
struct Registrar {

    explicit Registrar(const std::string& key) {
        FactoryType::instance().registerCreator(key, &Derived::create);
    }

};

}

#define MOFKA_REGISTER_IMPLEMENTATION_FOR(__factory__, __derived__, __name__) \
    static ::mofka::Registrar<__factory__, __derived__> \
    __mofkaRegistrarFor ## __factory__ ## _ ## __derived__ ## _ ## __name__{#__name__}

#endif
