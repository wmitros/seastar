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
 * Copyright 2020 ScyllaDB
 */

#pragma once

#include <seastar/core/iostream.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <string>
#include <strings.h>
#include <seastar/http/common.hh>
#include <seastar/http/exception.hh>
#include <seastar/util/log.hh>

namespace seastar {

namespace httpd {

class length_source_impl : public data_source_impl {
    input_stream<char>& _inp;
    size_t _read_bytes = 0;
    size_t _content_length;
public:
    length_source_impl(input_stream<char>& inp, size_t _length)
        : _inp(inp), _content_length(_length) {
    }
    virtual future<temporary_buffer<char>> get() override {
        if (_read_bytes == _content_length) {
            return make_ready_future<temporary_buffer<char>>();
        }
        return _inp.read_up_to(_content_length - _read_bytes).then([this](temporary_buffer<char> tmp_buf) {
            _read_bytes += tmp_buf.size();
            return tmp_buf;
        });
    }
    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        _read_bytes += std::min(n, _content_length - _read_bytes);
        return _inp.skip(std::min(n, _content_length - _read_bytes)).then([this] {
            return temporary_buffer<char>();
        });
    }
};

class chunked_source_impl : public data_source_impl {

    class chunk_parser {

        enum class parsing_state
            : bool {
                unfinished_length,
                finished_length,
        };

        temporary_buffer<char> _buf;
        size_t _read_length = 0;
        size_t _length;
        parsing_state _ps = parsing_state::unfinished_length;
        bool _eof = false;
        using consumption_result_type = consumption_result<char>;
    public:
        temporary_buffer<char> buf() {
            temporary_buffer<char> ret;
            std::swap(ret, _buf);
            _read_length += ret.size();
            return ret;
        }
        bool eof() const {
            return _eof;
        }

        future<consumption_result_type> operator()(temporary_buffer<char> data) {
            if (eof()) {
                return make_ready_future<consumption_result_type>(stop_consuming(std::move(data)));
            }
            // read the new data into _buf
            if (_buf.empty()) {
                _buf = std::move(data);
            } else {
                auto new_buf = temporary_buffer<char>(_buf.size() + data.size());
                std::copy(_buf.get(), _buf.get() + _buf.size(), new_buf.get_write());
                std::copy(data.get(), data.get() + data.size(), new_buf.get_write() + _buf.size());
                _buf = std::move(new_buf);
            }
            switch (_ps) {
                case parsing_state::unfinished_length: {
                    // can be entered multiple times before finishing parsing the chunk length
                    auto length_end = std::strstr(_buf.get(), "\r\n");
                    if ((length_end == nullptr && _buf.size() > 16) || length_end - _buf.get() > 16) {
                        return make_exception_future<consumption_result_type>(bad_chunk_exception("Chunk length too big"));
                    } else if (length_end == nullptr) {
                        return make_ready_future<consumption_result_type>(continue_consuming{});
                    }
                    char* err;
                    _length = strtol(_buf.get(), &err, 16);

                    if (length_end != err) {
                        //invalid characters before "\r\n"
                        return make_exception_future<consumption_result_type>(bad_chunk_exception(format("Can't read chunk length {}", std::string(_buf.get(), length_end - _buf.get()))));
                    }
                    size_t length_segment_size = length_end - _buf.get() + std::string("\r\n").size();
                    _buf.trim_front(length_segment_size);
                    _ps = parsing_state::finished_length;
                    [[fallthrough]];
                }
                case parsing_state::finished_length: {
                    if (_buf.empty()) {
                        // might happen only if we've just finished parsing the length
                        return make_ready_future<consumption_result_type>(continue_consuming{});
                    }
                    if (_read_length < _length) {
                        size_t to_read = std::min(_length - _read_length, _buf.size());
                        temporary_buffer<char> tmp = _buf.share(0, to_read);
                        _buf.trim_front(to_read);
                        std::swap(tmp, _buf);
                        return make_ready_future<consumption_result_type>(stop_consuming(std::move(tmp)));
                    }

                    if (_buf.size() < 2) {
                        // can't parse "\r\n"
                        return make_ready_future<consumption_result_type>(continue_consuming{});
                    }

                    if (std::strncmp(_buf.get(), "\r\n", 2) != 0) {
                        return make_exception_future<consumption_result_type>(bad_chunk_exception("The acutal chunk length exceeds the specified length"));
                    }
                    _buf.trim_front(2);

                    if (_length == 0) {
                        _eof = true;
                        return make_ready_future<consumption_result_type>(stop_consuming(buf()));
                    }
                    _ps = parsing_state::unfinished_length;
                    _read_length = 0;
                    return this->operator()(temporary_buffer<char>());
                }
            }
            __builtin_unreachable();
        }
    } _chunk;

    input_stream<char>& _inp;

public:
    chunked_source_impl(input_stream<char>& inp)
        : _inp(inp) {
    }

    virtual future<temporary_buffer<char>> get() override {
        return _inp.consume(_chunk).then([this] () mutable {
            return std::move(_chunk.buf());
        });
    }

    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        return do_with(size_t(0), temporary_buffer<char>(), [this, n] (auto& skipped, auto& buf) {
            return do_until([this, &skipped, n] { return skipped == n; }, [this, &skipped, n, &buf] {
                // we're skipping the parsed data, so we need to parse it first
                return this->get().then([this, &skipped, n, &buf] (temporary_buffer<char> tmp) {
                    size_t skipping = std::min(n - skipped, tmp.size());
                    skipped += skipping;
                    tmp.trim_front(skipping);
                    buf = std::move(tmp);
                });
            }).then([this, &buf] {
                return std::move(buf);
            });
        });
    }
};

} // namespace httpd

}
