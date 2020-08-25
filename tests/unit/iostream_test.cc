/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright (C) 2020 ScyllaDB.
 */

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <string>

using namespace seastar;

/*
 * Simple data source producing up to total_size bytes
 * in buffer_size-byte chunks.
 * */
class test_source_impl : public data_source_impl {
    size_t _buffer_size;
    size_t _remaining_size;
public:
    test_source_impl(size_t buffer_size, size_t total_size)
        : _buffer_size(buffer_size), _remaining_size(total_size) {
    }
    virtual future<temporary_buffer<char>> get() override {
        size_t len = std::min(_buffer_size, _remaining_size);
        temporary_buffer<char> tmp(len);
        memset(tmp.get_write(), 'a', len);
        _remaining_size -= len;
        return make_ready_future<temporary_buffer<char>>(std::move(tmp));
    }
    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        _remaining_size -= std::min(_remaining_size, n);
        return make_ready_future<temporary_buffer<char>>();
    }
};

SEASTAR_TEST_CASE(test_read_all) {
    return async([] {
        input_stream<char> inp(data_source(std::make_unique<test_source_impl>(5, 15)));
        BOOST_REQUIRE_EQUAL(to_sstring(inp.read_all().get0()), "aaaaaaaaaaaaaaa");
        BOOST_REQUIRE(inp.eof());
        input_stream<char> inp2(data_source(std::make_unique<test_source_impl>(5, 0)));
        BOOST_REQUIRE_EQUAL(to_sstring(inp2.read_all().get0()), "");
        BOOST_REQUIRE(inp2.eof());
    });
}

SEASTAR_TEST_CASE(test_skip_all) {
    return async([] {
        input_stream<char> inp(data_source(std::make_unique<test_source_impl>(5, 15)));
        inp.skip_all().get();
        BOOST_REQUIRE(inp.eof());
        BOOST_REQUIRE(to_sstring(inp.read().get0()).empty());
        input_stream<char> inp2(data_source(std::make_unique<test_source_impl>(5, 0)));
        inp2.skip_all().get();
        BOOST_REQUIRE(inp2.eof());
        BOOST_REQUIRE(to_sstring(inp2.read().get0()).empty());
    });
}
