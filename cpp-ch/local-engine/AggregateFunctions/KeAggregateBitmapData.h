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
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/PODArray.h>
#include "Storages/ReadBufferFromJavaBitmap.h"

// Include this header last, because it is an auto-generated dump of questionable
// garbage that breaks the build (e.g. it changes _POSIX_C_SOURCE).
// TODO: find out what it is. On github, they have proper interface headers like
// this one: https://github.com/RoaringBitmap/CRoaring/blob/master/include/roaring/roaring.h
#include <roaring.hh>
#include <roaring64map.hh>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int INCORRECT_DATA;
}
}

namespace local_engine
{
using namespace DB;

/**
  * Roaring bitmap data.
  * For a description of the roaring_bitmap_t, see: https://github.com/RoaringBitmap/CRoaring
  */
template <typename T>
class KeRoaringBitmapData : private boost::noncopyable
{
public:
    using RoaringBitmap = std::conditional_t<sizeof(T) >= 8, roaring::Roaring64Map, roaring::Roaring>;
    using Value = UInt64;
    RoaringBitmap roaring_bitmap;

    void add(T value) { roaring_bitmap.add(static_cast<Value>(value)); }

    UInt64 size() const { return roaring_bitmap.cardinality(); }

    void merge(const KeRoaringBitmapData & r1) { roaring_bitmap |= r1.roaring_bitmap; }

    void read(DB::ReadBuffer & in)
    {
        static thread_local String buf;
        size_t size;
        readVarUInt(size, in);

        static constexpr size_t max_size = 100_GiB;

        if (size == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect size (0) in groupBitmap.");
        if (size > max_size)
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size in groupBitmap (maximum: {})", max_size);

        if (in.available() > size)
        {
            roaring_bitmap = RoaringBitmap::readSafe(in.position(), size);
            in.ignore(size);
        }
        else
        {
            buf.reserve(size);
            in.readStrict(buf.data(), size);
            roaring_bitmap = RoaringBitmap::readSafe(buf.data(), size);
        }
    }

    void write(DB::WriteBuffer & out) const
    {
        auto size = roaring_bitmap.getSizeInBytes();
        writeVarUInt(size, out);
        if (out.available() > size)
        {
            roaring_bitmap.write(out.position());
            out.position() += size;
        }
        else
        {
            std::unique_ptr<char[]> buf(new char[size]);
            roaring_bitmap.write(buf.get());
            out.write(buf.get(), size);
        }
    }

    void to_ke_bitmap_data(DB::WriteBuffer & ke_bitmap_data_buffer) const
    {
        auto size = roaring_bitmap.getSizeInBytes();

        std::unique_ptr<char[]> buf(new char[size]);
        roaring_bitmap.write(buf.get());

        Int8 signedLongs = 0;
        writeBinary(signedLongs, ke_bitmap_data_buffer);

        writeBinary(static_cast<unsigned char>(buf.get()[3]), ke_bitmap_data_buffer);
        writeBinary(static_cast<unsigned char>(buf.get()[2]), ke_bitmap_data_buffer);
        writeBinary(static_cast<unsigned char>(buf.get()[1]), ke_bitmap_data_buffer);
        writeBinary(static_cast<unsigned char>(buf.get()[0]), ke_bitmap_data_buffer);

        auto bitmap_data = buf.get() + 8;
        ke_bitmap_data_buffer.write(bitmap_data, size - 8);
    }

    /**
     * Computes the intersection between two bitmaps
     */
    void rb_and(const KeRoaringBitmapData & r1) /// NOLINT
    {
        roaring_bitmap &= r1.roaring_bitmap;
    }

    /**
     * Computes the union between two bitmaps.
     */
    void rb_or(const KeRoaringBitmapData & r1)
    {
        merge(r1); /// NOLINT
    }

    /**
     * Computes the symmetric difference (xor) between two bitmaps.
     */
    void rb_xor(const KeRoaringBitmapData & r1) /// NOLINT
    {
        roaring_bitmap ^= r1.roaring_bitmap;
    }

    /**
     * Computes the difference (andnot) between two bitmaps
     */
    void rb_andnot(const KeRoaringBitmapData & r1) /// NOLINT
    {
        roaring_bitmap -= r1.roaring_bitmap;
    }

    /**
     * Computes the cardinality of the intersection between two bitmaps.
     */
    UInt64 rb_and_cardinality(const KeRoaringBitmapData & r1) const /// NOLINT
    {
        return (roaring_bitmap & r1.roaring_bitmap).cardinality();
    }

    /**
     * Computes the cardinality of the union between two bitmaps.
     */
    UInt64 rb_or_cardinality(const KeRoaringBitmapData & r1) const /// NOLINT
    {
        UInt64 c1 = size();
        UInt64 c2 = r1.size();
        UInt64 inter = rb_and_cardinality(r1);
        return c1 + c2 - inter;
    }

    /**
     * Computes the cardinality of the symmetric difference (andnot) between two bitmaps.
     */
    UInt64 rb_xor_cardinality(const KeRoaringBitmapData & r1) const /// NOLINT
    {
        UInt64 c1 = size();
        UInt64 c2 = r1.size();
        UInt64 inter = rb_and_cardinality(r1);
        return c1 + c2 - 2 * inter;
    }

    /**
     * Computes the cardinality of the difference (andnot) between two bitmaps.
     */
    UInt64 rb_andnot_cardinality(const KeRoaringBitmapData & r1) const /// NOLINT
    {
        UInt64 c1 = size();
        UInt64 inter = rb_and_cardinality(r1);
        return c1 - inter;
    }

    /**
     * Return 1 if the two bitmaps contain the same elements.
     */
    UInt8 rb_equals(const KeRoaringBitmapData & r1) /// NOLINT
    {
        return roaring_bitmap == r1.roaring_bitmap;
    }

    /**
     * Check whether two bitmaps intersect.
     * Intersection with an empty set is always 0 (consistent with hasAny).
     */
    UInt8 rb_intersect(const KeRoaringBitmapData & r1) const /// NOLINT
    {
        if ((roaring_bitmap & r1.roaring_bitmap).cardinality() > 0)
            return 1;
        return 0;
    }

    /**
     * Check whether the argument is the subset of this set.
     * Empty set is a subset of any other set (consistent with hasAll).
     * It's used in subset and currently only support comparing same type
     */
    UInt8 rb_is_subset(const KeRoaringBitmapData & r1) const /// NOLINT
    {
        if (!r1.roaring_bitmap.isSubset(roaring_bitmap))
            return 0;
        return 1;
    }

    /**
     * Check whether this bitmap contains the argument.
     */
    UInt8 rb_contains(UInt64 x) const /// NOLINT
    {
        if (!std::is_same_v<T, UInt64> && x > rb_max())
            return 0;

        UInt32 high_bytes = uint32_t(x >> 32);
        UInt32 high_bytes_new
            = ((high_bytes >> 24)) | ((high_bytes >> 8) & 0xFF00) | ((high_bytes << 8) & 0xFF0000) | ((high_bytes << 24));
        UInt64 value = (uint64_t(high_bytes_new) << 32) | uint64_t(uint32_t(x));

        return roaring_bitmap.contains(value);
    }

    /**
     * Convert elements to integer array, return number of elements
     */
    template <typename Element>
    UInt64 rb_to_array(PaddedPODArray<Element> & res) const /// NOLINT
    {
        UInt64 count = 0;
        for (auto it = roaring_bitmap.begin(); it != roaring_bitmap.end(); ++it)
        {
            // reverse high 4 bytes to Little-endian
            Int64 original_value = *it;
            UInt32 high_bytes = uint32_t(original_value >> 32);
            UInt32 high_bytes_new
                = ((high_bytes >> 24)) | ((high_bytes >> 8) & 0xFF00) | ((high_bytes << 8) & 0xFF0000) | ((high_bytes << 24));
            Int64 value = (uint64_t(high_bytes_new) << 32) | uint64_t(uint32_t(original_value));
            res.emplace_back(value);
            ++count;
        }
        return count;
    }

    /**
     * Return new set with specified range (not include the range_end)
     * It's used in subset and currently only support UInt32
     */
    UInt64 rb_range(UInt64 range_start, UInt64 range_end, KeRoaringBitmapData & r1) const /// NOLINT
    {
        UInt64 count = 0;
        if (range_start >= range_end)
            return count;

        for (auto it = roaring_bitmap.begin(); it != roaring_bitmap.end(); ++it)
        {
            if (*it < range_start)
                continue;

            if (*it < range_end)
            {
                r1.add(*it);
                ++count;
            }
            else
                break;
        }
        return count;
    }

    /**
     * Return new set of the smallest `limit` values in set which is no less than `range_start`.
     * It's used in subset and currently only support UInt32
     */
    UInt64 rb_limit(UInt64 range_start, UInt64 limit, KeRoaringBitmapData & r1) const /// NOLINT
    {
        if (limit == 0)
            return 0;

        UInt64 count = 0;
        for (auto it = roaring_bitmap.begin(); it != roaring_bitmap.end(); ++it)
        {
            if (*it < range_start)
                continue;

            if (count < limit)
            {
                r1.add(*it);
                ++count;
            }
            else
                break;
        }
        return count;
    }

    UInt64 rb_offset_limit(UInt64 offset, UInt64 limit, KeRoaringBitmapData & r1) const /// NOLINT
    {
        if (limit == 0 || offset >= size())
            return 0;

        UInt64 count = 0;
        UInt64 offset_count = 0;
        auto it = roaring_bitmap.begin();
        for (; it != roaring_bitmap.end() && offset_count < offset; ++it)
            ++offset_count;

        for (; it != roaring_bitmap.end() && count < limit; ++it, ++count)
            r1.add(*it);
        return count;
    }

    UInt64 rb_min() const /// NOLINT
    {
        return roaring_bitmap.minimum();
    }

    UInt64 rb_max() const /// NOLINT
    {
        return roaring_bitmap.maximum();
    }

    /**
     * Replace value.
     * It's used in transform and currently can only support UInt32
     */
    void rb_replace(const UInt64 * from_vals, const UInt64 * to_vals, size_t num) /// NOLINT
    {
        for (size_t i = 0; i < num; ++i)
        {
            if (from_vals[i] == to_vals[i])
                continue;
            bool changed = roaring_bitmap.removeChecked(static_cast<Value>(from_vals[i]));
            if (changed)
                roaring_bitmap.add(static_cast<Value>(to_vals[i]));
        }
    }
};

template <typename T>
struct KeAggregateBitmapData
{
    // If false, all bitmap operations will be treated as merge to initialize the state
    bool init = false;
    KeRoaringBitmapData<T> roaring_bitmap;
    static const char * name() { return "keBitmap"; }
};

}
