/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#include "ReadBufferFromJavaBitmap.h"
#include <optional>
#include <IO/ReadBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

namespace local_engine
{

ReadBufferFromJavaBitmap::ReadBufferFromJavaBitmap(char * original_ptr, size_t original_data_size) : DB::ReadBuffer(original_ptr, 0)
{
    original_size = original_data_size;
    ch_bitmap_data_buffer = std::make_unique<WriteBufferFromOwnString>();
    writeVarUInt(original_size + 3, *ch_bitmap_data_buffer);
    Int64 map_size = 0;
    map_size += map_size << 8 | static_cast<unsigned char>(original_ptr[1]);
    map_size += map_size << 8 | static_cast<unsigned char>(original_ptr[2]);
    map_size += map_size << 8 | static_cast<unsigned char>(original_ptr[3]);
    map_size += map_size << 8 | static_cast<unsigned char>(original_ptr[4]);
    writeBinary(map_size, *ch_bitmap_data_buffer);

    BufferBase::set(ch_bitmap_data_buffer->str().data(), ch_bitmap_data_buffer->str().size(), 0);

    java_bitmap_data = original_ptr + 5;
}

bool ReadBufferFromJavaBitmap::nextImpl()
{
    if (!read_header_finished)
    {
        read_header_finished = true;
        BufferBase::set(java_bitmap_data, original_size - 5, 0);
        return true;
    }
    return false;
}
}
