/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __ALPHA_ASYNC_REQUEST_IMPL_H
#define __ALPHA_ASYNC_REQUEST_IMPL_H

#include <functional>
#include <thallium.hpp>

namespace alpha {

namespace tl = thallium;

struct AsyncRequestImpl {

    AsyncRequestImpl(tl::async_response&& async_response)
    : m_async_response(std::move(async_response)) {}

    tl::async_response                     m_async_response;
    bool                                   m_waited = false;
    std::function<void(AsyncRequestImpl&)> m_wait_callback;

};

}

#endif
