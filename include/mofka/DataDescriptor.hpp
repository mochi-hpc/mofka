/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_DATA_DESCRIPTOR_HPP
#define MOFKA_DATA_DESCRIPTOR_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Exception.hpp>
#include <mofka/Archive.hpp>

#include <memory>
#include <string_view>
#include <map>

namespace mofka {

class DataDescriptorImpl;

/**
 * @brief A DataDescriptor is an opaque object describing
 * how the data is stored in some Storage backend in Mofka.
 */
class DataDescriptor {

    public:

    struct Segment {
        std::size_t offset;
        std::size_t size;
    };

    /**
     * @brief Creates a NULL DataDescriptor.
     */
    static DataDescriptor Null();

    /**
     * @brief Create an implementation-dependent DataDescriptor.
     *
     * @param location Implementation-dependent representation of the data location.
     * @param size Size of the underlying data.
     *
     * @return a DataDescriptor.
     */
    static DataDescriptor From(std::string_view location, size_t size);

    /**
     * @brief Constructor (equivalent to a Null DataDescriptor).
     */
    DataDescriptor();

    /**
     * @brief Copy-constructor.
     */
    DataDescriptor(const DataDescriptor&);

    /**
     * @brief Move-constructor.
     */
    DataDescriptor(DataDescriptor&&);

    /**
     * @brief Copy-assignment operator.
     */
    DataDescriptor& operator=(const DataDescriptor&);

    /**
     * @brief Move-assignment operator.
     */
    DataDescriptor& operator=(DataDescriptor&&);

    /**
     * @brief Destructor.
     */
    ~DataDescriptor();

    /**
     * @brief Return the size of the underlying data in bytes.
     */
    size_t size() const;

    /**
     * @brief Returns the root location (interpretable by the
     * PartitionManager that created this DataDescriptor).
     */
    const std::vector<char>& location() const;

    /**
     * @brief Returns the root location (interpretable by the
     * PartitionManager that created this DataDescriptor).
     */
    std::vector<char>& location();

    /**
     * @brief Extract a flat representation of the data descriptor.
     */
    std::vector<Segment> flatten() const;

    /**
     * @brief Create a DataDescriptor representing a subset of
     * the data represented by this descriptor.
     *
     * @param offset Offset at which to start the view.
     * @param numblocks Number of blocks to take.
     * @param blocksize Size of each block.
     * @param gapsize Distance between the end of a block
     * and the beginning of the next one.
     *
     * @return a new DataDescriptor.
     *
     * Example: let's assume the current DataDescriptor D represents
     * a region of memory containing the following data:
     *
     * "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
     *   **   **   **   **   **
     *
     * Calling D.makeStridedView(1, 5, 2, 3) will select the bytes shown with
     * a * above, leading to a DataDescriptor representing the following data:
     *
     * "BCGHLMQRVW"
     *
     * We have 5 blocks of length 2 with a gap of 3 between each block,
     * starting at an offset of 1 byte.
     */
    DataDescriptor makeStridedView(
        size_t offset,
        size_t numblocks,
        size_t blocksize,
        size_t gapsize) const;

    /**
     * @brief This function takes a subset of the initial DataDescriptor
     * by selecting a contiguous segment of the specified size starting
     * at the specified offset.
     *
     * @param offset Offset of the view.
     * @param size Size of the view.
     *
     * @return a new DataDescriptor.
     *
     * Example: let's assume the current DataDescriptor D represents
     * a region of memory containing the following data:
     *
     * "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
     *      ********
     *
     * Calling D.makeSubView(4, 8) will select the bytes shown with
     * a * above, leading to a DataDescriptor representing the following data:
     *
     * "EFGHIJKL"
     */
    DataDescriptor makeSubView(
        size_t offset,
        size_t size) const;

    /**
     * @brief This function takes a map associating an offset to a size
     * and creates a view by selecting the segments (offset, size) in
     * the underlying DataDescriptor.
     *
     * @warning: segments must not overlap.
     *
     * @note: the use of an std::map forces segments to be sorted by offset.
     *
     * @note: an unstructured DataDescriptor is more difficult to
     * handle and store than a structured (sub or strided) one, so do not
     * use this function if you have the possibility to use sub or strided
     * views (or a composition of them).
     *
     * @param segments List of <offset,size> pairs.
     *
     * @return a new DataDescriptor.
     *
     * Example: let's assume the current DataDescriptor D represents
     * a region of memory containing the following data:
     *
     * "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
     *   ***   *****  **
     *
     * Let M be a list with the content {{1, 3}, {7, 5}, {14, 2}}.
     * Calling D.makeUnstructuredView(M) will select the bytes shown with
     * a * above, leading to a DataDescriptor representing the following data:
     *
     * "BCDHIJKLOPQR"
     */
    DataDescriptor makeUnstructuredView(
        const std::vector<std::pair<size_t, size_t>>& segments) const;

    /**
     * @brief Load the DataDescriptor from an Archive.
     *
     * @param ar Archive.
     */
    void load(Archive& ar);

    /**
     * @brief Serialize the DataDescriptor into an Archive.
     *
     * @param ar Archive.
     */
    void save(Archive& ar) const;

    /**
     * @brief Checks if the Data instance is valid.
     */
    operator bool() const;



    private:

    /**
     * @brief Constructor is private.
     *
     * @param impl Pointer to implementation.
     */
    DataDescriptor(const std::shared_ptr<DataDescriptorImpl>& impl);

    std::shared_ptr<DataDescriptorImpl> self;

    friend struct Cerealized<DataDescriptor>;
};

}

#endif
