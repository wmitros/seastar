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

#include <seastar/http/chunk_parsers.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/http/common.hh>
#include <seastar/http/exception.hh>

namespace seastar {

namespace httpd {

namespace internal {


struct content_stream_state {
    bool closed = false;
    bool eof = false;
    std::unordered_map<sstring, sstring> extensions;
    std::unordered_map<sstring, sstring> trailing_headers;
};

/*
 * An input_stream wrapper that allows to read only "length" bytes
 * from it, used to handle requests with large bodies.
 * */
class content_length_source_impl : public data_source_impl {
    input_stream<char>& _inp;
    content_stream_state& _state;
    size_t _remaining_bytes = 0;
public:
    content_length_source_impl(input_stream<char>& inp, content_stream_state& state, size_t length)
        : _inp(inp), _state(state), _remaining_bytes(length) {
            if (_remaining_bytes == 0) {
                _state.eof = true;
            }
    }

    virtual future<temporary_buffer<char>> get() override {
        if (_remaining_bytes == 0) {
            return make_ready_future<temporary_buffer<char>>();
        }
        return _inp.read_up_to(_remaining_bytes).then([this] (temporary_buffer<char> tmp_buf) {
            _remaining_bytes -= tmp_buf.size();
            if (_remaining_bytes == 0) {
                _state.eof = true;
            }
            return tmp_buf;
        });
    }

    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        uint64_t skip_bytes = std::min(n, _remaining_bytes);
        _remaining_bytes -= skip_bytes;
        return _inp.skip(skip_bytes).then([this] {
            if (_remaining_bytes == 0) {
                _state.eof = true;
            }
            return temporary_buffer<char>();
        });
    }

    virtual future<> close() override {
        _state.closed = true;
        return make_ready_future<>();
    }
};

/*
 * An input_stream wrapper that decodes a request body
 * with "chunked" encoding.
 * */
class chunked_source_impl : public data_source_impl {
    class chunk_parser {
        enum class parsing_state
            : uint8_t {
                size_and_ext,
                body,
                trailer_part
        };
        http_chunk_size_and_ext_parser _size_and_ext_parser;
        http_chunk_trailer_parser _trailer_parser;

        temporary_buffer<char> _buf;
        size_t _read_length = 0;
        size_t _length;
        parsing_state _ps = parsing_state::size_and_ext;
        bool _eof = false;
        std::unordered_map<sstring, sstring> _extensions;
        std::unordered_map<sstring, sstring> _trailing_headers;
        using consumption_result_type = consumption_result<char>;
    public:
        temporary_buffer<char> buf() {
            _read_length += _buf.size();
            return std::move(_buf);
        }

        bool eof() const {
            return _buf.empty() && _eof;
        }

        void update_state(content_stream_state& state) {
            state.eof = eof();
            state.extensions.merge(_extensions);
            for (auto& key_val : _extensions) {
                state.extensions[key_val.first] += sstring(",") + key_val.second;
            }
            _extensions = std::unordered_map<sstring,sstring>();
            state.trailing_headers.merge(_trailing_headers);
            for (auto& key_val : _trailing_headers) {
                state.trailing_headers[key_val.first] += sstring(",") + key_val.second;
            }
            _trailing_headers = std::unordered_map<sstring,sstring>();
        }

        future<consumption_result_type> operator()(temporary_buffer<char> data) {
            if (_buf.size() || _eof) {
                return make_ready_future<consumption_result_type>(stop_consuming(std::move(data)));
            }
            switch (_ps) {
            case parsing_state::size_and_ext:
                _size_and_ext_parser.init();
                return _size_and_ext_parser(std::move(data)).then([this] (std::optional<temporary_buffer<char>> res) {
                    if (res.has_value()) {
                        if (_size_and_ext_parser.failed()) {
                            return make_exception_future<consumption_result_type>(bad_request_exception("Can't parse chunk size and extensions"));
                        }
                        // save extensions
                        auto parsed_extensions = _size_and_ext_parser.get_parsed_extensions();
                        _extensions.merge(parsed_extensions);
                        for (auto& key_val : parsed_extensions) {
                            _extensions[key_val.first] += sstring(",") + key_val.second;
                        }

                        // save size
                        auto size_string = _size_and_ext_parser.get_size();
                        if (size_string.size() > 16) {
                            return make_exception_future<consumption_result_type>(bad_chunk_exception("Chunk length too big"));
                        }
                        _read_length = 0;
                        _length = strtol(size_string.c_str(), nullptr, 16);

                        if (_length == 0) {
                            _ps = parsing_state::trailer_part;
                        } else {
                            _ps = parsing_state::body;
                            if (res.value().empty()) {
                                return make_ready_future<consumption_result_type>(continue_consuming{});
                            }
                        }
                        return this->operator()(std::move(res.value()));
                    } else {
                        return make_ready_future<consumption_result_type>(continue_consuming{});
                    }
                });
            case parsing_state::body:
                // read the new data into _buf
                if (_buf.size()) {
                    // input consumed but not read
                    return make_ready_future<consumption_result_type>(stop_consuming(std::move(data)));
                }

                if (_read_length < _length) {
                    size_t to_read = std::min(_length - _read_length, data.size());
                    _buf = data.share(0, to_read);
                    data.trim_front(to_read);
                    return make_ready_future<consumption_result_type>(stop_consuming(std::move(data)));
                }

                // chunk body is finished
                if (_read_length == _length) {
                    // we haven't read \r yet
                    if (data.get()[0] != '\r') {
                        return make_exception_future<consumption_result_type>(bad_chunk_exception("The acutal chunk length exceeds the specified length"));
                    } else {
                        _read_length++;
                        data.trim_front(1);
                        if (data.empty()) {
                            return make_ready_future<consumption_result_type>(continue_consuming{});
                        }
                    }
                }
                if (_read_length == _length + 1) {
                    // we haven't read \n but have \r
                    if (data.get()[0] != '\n') {
                        return make_exception_future<consumption_result_type>(bad_chunk_exception("The acutal chunk length exceeds the specified length"));
                    } else {
                        _ps = parsing_state::size_and_ext;
                        data.trim_front(1);
                        if (data.empty()) {
                            return make_ready_future<consumption_result_type>(continue_consuming{});
                        }
                    }
                }
                return this->operator()(std::move(data));
            case parsing_state::trailer_part:
                _trailer_parser.init();
                return _trailer_parser(std::move(data)).then([this] (std::optional<temporary_buffer<char>> res) {
                    if (res.has_value()) {
                        if (_trailer_parser.failed()) {
                            return make_exception_future<consumption_result_type>(bad_request_exception("Can't parse chunked request trailer"));
                        }
                        // save trailing headers
                        auto parsed_headers = _trailer_parser.get_parsed_headers();
                        _trailing_headers.merge(parsed_headers);
                        for (auto& key_val : parsed_headers) {
                            _trailing_headers[key_val.first] += sstring(",") + key_val.second;
                        }
                        _eof = true;
                        return this->operator()(std::move(res.value()));
                    } else {
                        return make_ready_future<consumption_result_type>(continue_consuming{});
                    }
                });
            }
            __builtin_unreachable();
        }
    } _chunk;

    input_stream<char>& _inp;
    content_stream_state& _state;

public:
    chunked_source_impl(input_stream<char>& inp, content_stream_state& state)
        : _inp(inp), _state(state) {
    }

    virtual future<temporary_buffer<char>> get() override {
        return _inp.consume(_chunk).then([this] () mutable {
            _chunk.update_state(_state);
            return std::move(_chunk.buf());
        });
    }

    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        return do_with(size_t(0), temporary_buffer<char>(), [this, n] (size_t& skipped, temporary_buffer<char>& buf) {
            return do_until([this, &skipped, n] { return skipped == n; }, [this, &skipped, n, &buf] {
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

    virtual future<> close() override {
        _state.closed = true;
        return make_ready_future<>();
    }
};

} // namespace internal

} // namespace httpd

}
