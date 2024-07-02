/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_DATA_HPP
#define MOFKA_DATA_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Exception.hpp>

#include <functional>
#include <memory>
#include <vector>

namespace mofka {

class DataImpl;
class PythonBindingHelper;
class ProducerBatchImpl;
class ConsumerImpl;

/**
 * @brief A Data is an object that encapsulates the data of an event.
 */
class Data {

    public:

    struct Segment {
        void*  ptr;
        size_t size;
    };

    using Context = void*;
    using FreeCallback = std::function<void(Context)>;

    /**
     * @brief Constructor. The resulting Data handle will represent NULL.
     */
    Data(Context ctx = nullptr, FreeCallback free_cb = FreeCallback{});

    /**
     * @brief Creates a Data object with a single segment.
     */
    Data(void* ptr, size_t size, Context ctx = nullptr, FreeCallback free_cb = FreeCallback{});

    /**
     * @brief Creates a Data object from a list of Segments.
     */
    Data(std::vector<Segment> segments, Context ctx = nullptr, FreeCallback free_cb = FreeCallback{});

    /**
     * @brief Copy-constructor.
     */
    Data(const Data&);

    /**
     * @brief Move-constructor.
     */
    Data(Data&&);

    /**
     * @brief Copy-assignment operator.
     */
    Data& operator=(const Data&);

    /**
     * @brief Move-assignment operator.
     */
    Data& operator=(Data&&);

    /**
     * @brief Free.
     */
    ~Data();

    /**
     * @brief Returns the list of memory segments
     * this Data object refers to.
     */
    const std::vector<Segment>& segments() const;

    /**
     * @brief Return the total size of the Data.
     */
    size_t size() const;

    /**
     * @brief Checks if the Data instance is valid.
     */
    operator bool() const;

    /**
     * @brief Return the context of this Data object.
     */
    Context context() const;

    private:

    /**
     * @brief Constructor is private.
     *
     * @param impl Pointer to implementation.
     */
    Data(const std::shared_ptr<DataImpl>& impl);

    std::shared_ptr<DataImpl> self;

    friend class ProducerBatchImpl;
    friend class Event;
    friend class ConsumerImpl;

};

}

#endif
