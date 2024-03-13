#ifndef BENCHMARK_COMMUNICATOR_H
#define BENCHMARK_COMMUNICATOR_H

#include <nlohmann/json.hpp>
#include <mpi.h>
#include <thallium.hpp>

class Communicator {

    using json = nlohmann::json;

    thallium::pool m_pool;
    MPI_Comm       m_comm;

    public:

    Communicator(thallium::pool pool, MPI_Comm comm)
    : m_pool(pool)
    , m_comm(comm)
    {}

    Communicator(const Communicator&) = default;

    int size() const {
        int s;
        MPI_Comm_size(m_comm, &s);
        return s;
    }

    int rank() const {
        int r;
        MPI_Comm_rank(m_comm, &r);
        return r;
    }

    void barrier() {
        m_pool.make_thread([this](){ MPI_Barrier(m_comm);})->join();
    }

    void bcast(char* buffer, int count, int root) {
        m_pool.make_thread([this, buffer, count, root](){
            MPI_Bcast(buffer, count, MPI_BYTE, root, m_comm);})->join();
    }

    Communicator split(int color) {
        int r = rank();
        MPI_Comm newcomm;
        m_pool.make_thread([this, color, &newcomm, r](){
            MPI_Comm_split(m_comm, color, r, &newcomm);
        })->join();
        return Communicator{m_pool, newcomm};
    }

    void free() {
        MPI_Comm_free(&m_comm);
    }

};

#endif
