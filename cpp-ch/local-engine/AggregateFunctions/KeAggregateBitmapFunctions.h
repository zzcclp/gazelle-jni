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
#pragma once

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/Combinators/AggregateFunctionNull.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/ReadBufferFromJavaBitmap.h>
#include <AggregateFunctions/KeAggregateBitmapData.h>
#include <Common/assert_cast.h>

namespace local_engine
{

using namespace DB;

// For handle null values
template <bool result_is_nullable, bool serialize_flag>
class SparkAggregateBitmapNullUnary final
    : public AggregateFunctionNullBase<result_is_nullable, serialize_flag,
                                       SparkAggregateBitmapNullUnary<result_is_nullable, serialize_flag>>
{
public:
    SparkAggregateBitmapNullUnary(AggregateFunctionPtr nested_function_, const DataTypes & arguments, const Array & params)
        : AggregateFunctionNullBase<result_is_nullable, serialize_flag,
                                    SparkAggregateBitmapNullUnary<result_is_nullable, serialize_flag>>(std::move(nested_function_), arguments, params)
    {
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const ColumnNullable * column = assert_cast<const ColumnNullable *>(columns[0]);
        const IColumn * nested_column = &column->getNestedColumn();
        this->nested_function->add(place, &nested_column, row_num, arena);
    }
};

template <typename T, typename Data>
class KeAggregateBitmapOrCardinality final : public IAggregateFunctionDataHelper<Data, KeAggregateBitmapOrCardinality<T, Data>>
{
public:
    explicit KeAggregateBitmapOrCardinality(const DB::DataTypePtr & type)
        : IAggregateFunctionDataHelper<Data, KeAggregateBitmapOrCardinality<T, Data>>({type}, {}, createResultType())
    {
    }

    ~KeAggregateBitmapOrCardinality() override
    {
    }

    String getName() const override { return "ke_bitmap_or_cardinality"; }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeNumber<T>>(); }

    bool allocatesMemoryInArena() const override { return false; }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function, const DataTypes & types, const Array & params, const AggregateFunctionProperties & /* properties */) const override
    {
        return std::make_shared<SparkAggregateBitmapNullUnary<false, false>>(nested_function, types, params);
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) Data;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        Data & data_lhs = this->data(place);
        auto bitmap_data = assert_cast<const ColumnString &>(*columns[0]).getDataAt(row_num);

        if (!data_lhs.init)
        {
            data_lhs.init = true;
            // Null data will be skip in AggregateFunctionNullUnary.add()
            if (!bitmap_data.empty())
            {
                auto charBuff = std::make_unique<ReadBufferFromJavaBitmap>(const_cast<char *>(bitmap_data.data), bitmap_data.size);
                data_lhs.roaring_bitmap.read(*charBuff);
            }
        }
        else
        {
            if (!bitmap_data.empty())
            {
                auto data_rhs = std::make_unique<Data>();
                auto charBuff = std::make_unique<ReadBufferFromJavaBitmap>(const_cast<char *>(bitmap_data.data), bitmap_data.size);
                data_rhs->roaring_bitmap.read(*charBuff);
                data_lhs.roaring_bitmap.merge(data_rhs->roaring_bitmap);
            }
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        Data & data_lhs = this->data(place);
        const Data & data_rhs = this->data(rhs);

        if (!data_rhs.init)
            return;

        if (!data_lhs.init)
        {
            data_lhs.init = true;
        }
        data_lhs.roaring_bitmap.merge(data_rhs.roaring_bitmap);
    }

    bool isVersioned() const override { return false; }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).roaring_bitmap.write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).init = true;
        this->data(place).roaring_bitmap.read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnVector<T> &>(to).getData().push_back(static_cast<T>(this->data(place).roaring_bitmap.size()));
    }
};

template <typename T, typename Data>
class KeAggregateBitmapOr final : public IAggregateFunctionDataHelper<Data, KeAggregateBitmapOr<T, Data>>
{
public:
    explicit KeAggregateBitmapOr(const DB::DataTypePtr & type)
        : IAggregateFunctionDataHelper<Data, KeAggregateBitmapOr<T, Data>>({type}, {}, createResultType())
    {
    }

    String getName() const override { return "ke_bitmap_or_data"; }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeString>(); }

    bool allocatesMemoryInArena() const override { return false; }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function, const DataTypes & types, const Array & params, const AggregateFunctionProperties & /*properties*/) const override
    {
        return std::make_shared<SparkAggregateBitmapNullUnary<false, false>>(nested_function, types, params);
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) Data;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        Data & data_lhs = this->data(place);
        auto bitmap_data = assert_cast<const ColumnString &>(*columns[0]).getDataAt(row_num);

        if (!data_lhs.init)
        {
            data_lhs.init = true;
            // Null data will be skip in AggregateFunctionNullUnary.add()
            if (!bitmap_data.empty())
            {
                auto charBuff = std::make_unique<ReadBufferFromJavaBitmap>(const_cast<char *>(bitmap_data.data), bitmap_data.size);
                data_lhs.roaring_bitmap.read(*charBuff);
            }
        }
        else
        {
            if (!bitmap_data.empty())
            {
                auto data_rhs = std::make_unique<Data>();
                auto charBuff = std::make_unique<ReadBufferFromJavaBitmap>(const_cast<char *>(bitmap_data.data), bitmap_data.size);
                data_rhs->roaring_bitmap.read(*charBuff);
                data_lhs.roaring_bitmap.rb_or(data_rhs->roaring_bitmap);
            }
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        Data & data_lhs = this->data(place);
        const Data & data_rhs = this->data(rhs);

        if (!data_rhs.init)
            return;

        if (!data_lhs.init)
        {
            data_lhs.init = true;
        }
        data_lhs.roaring_bitmap.rb_or(data_rhs.roaring_bitmap);
    }

    bool isVersioned() const override { return false; }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).roaring_bitmap.write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).init = true;
        this->data(place).roaring_bitmap.read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        std::string str;
        auto write_buffer = std::make_unique<WriteBufferFromVector<std::string>>(str);
        this->data(place).roaring_bitmap.to_ke_bitmap_data(*write_buffer);
        assert_cast<ColumnString &>(to).insert(str);
    }
};

template <typename T, typename Data>
class KeAggregateBitmapAndValue final : public IAggregateFunctionDataHelper<Data, KeAggregateBitmapAndValue<T, Data>>
{
public:
    explicit KeAggregateBitmapAndValue(const DB::DataTypePtr & type)
        : IAggregateFunctionDataHelper<Data, KeAggregateBitmapAndValue<T, Data>>({type}, {}, createResultType())
    {
    }

    String getName() const override { return "ke_bitmap_and_value"; }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeNumber<T>>(); }

    bool allocatesMemoryInArena() const override { return false; }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function, const DataTypes & types, const Array & params, const AggregateFunctionProperties & /*properties*/) const override
    {
        return std::make_shared<SparkAggregateBitmapNullUnary<false, false>>(nested_function, types, params);
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) Data;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        Data & data_lhs = this->data(place);
        auto bitmap_data = assert_cast<const ColumnString &>(*columns[0]).getDataAt(row_num);

        if (!data_lhs.init)
        {
            data_lhs.init = true;
            // Null data will be skip in AggregateFunctionNullUnary.add()
            if (!bitmap_data.empty())
            {
                auto charBuff = std::make_unique<ReadBufferFromJavaBitmap>(const_cast<char *>(bitmap_data.data), bitmap_data.size);
                data_lhs.roaring_bitmap.read(*charBuff);
            }
        }
        else
        {
            auto data_rhs = std::make_unique<Data>();
            if (!bitmap_data.empty())
            {
                auto charBuff = std::make_unique<ReadBufferFromJavaBitmap>(const_cast<char *>(bitmap_data.data), bitmap_data.size);
                data_rhs->roaring_bitmap.read(*charBuff);
            }
            data_lhs.roaring_bitmap.rb_and(data_rhs->roaring_bitmap);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        Data & data_lhs = this->data(place);
        const Data & data_rhs = this->data(rhs);

        if (!data_rhs.init)
            return;

        if (!data_lhs.init)
        {
            data_lhs.init = true;
            data_lhs.roaring_bitmap.merge(data_rhs.roaring_bitmap);
        }
        else
        {
            data_lhs.roaring_bitmap.rb_and(data_rhs.roaring_bitmap);
        }
    }

    bool isVersioned() const override { return false; }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).roaring_bitmap.write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).init = true;
        this->data(place).roaring_bitmap.read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnVector<T> &>(to).getData().push_back(static_cast<T>(this->data(place).roaring_bitmap.size()));
    }
};

template <typename T, typename Data>
class KeAggregateBitmapAndIds final : public IAggregateFunctionDataHelper<Data, KeAggregateBitmapAndIds<T, Data>>
{
public:
    explicit KeAggregateBitmapAndIds(const DB::DataTypePtr & type)
        : IAggregateFunctionDataHelper<Data, KeAggregateBitmapAndIds<T, Data>>({type}, {}, createResultType())
    {
    }

    String getName() const override { return "ke_bitmap_and_ids"; }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeNumber<T>>()); }

    bool allocatesMemoryInArena() const override { return false; }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function, const DataTypes & types, const Array & params, const AggregateFunctionProperties & /*properties*/) const override
    {
        return std::make_shared<SparkAggregateBitmapNullUnary<false, false>>(nested_function, types, params);
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) Data;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        Data & data_lhs = this->data(place);
        auto bitmap_data = assert_cast<const ColumnString &>(*columns[0]).getDataAt(row_num);

        if (!data_lhs.init)
        {
            data_lhs.init = true;
            // Null data will be skip in AggregateFunctionNullUnary.add()
            if (!bitmap_data.empty())
            {
                auto charBuff = std::make_unique<ReadBufferFromJavaBitmap>(const_cast<char *>(bitmap_data.data), bitmap_data.size);
                data_lhs.roaring_bitmap.read(*charBuff);
            }
        }
        else
        {
            auto data_rhs = std::make_unique<Data>();
            if (!bitmap_data.empty())
            {
                auto charBuff = std::make_unique<ReadBufferFromJavaBitmap>(const_cast<char *>(bitmap_data.data), bitmap_data.size);
                data_rhs->roaring_bitmap.read(*charBuff);
            }
            data_lhs.roaring_bitmap.rb_and(data_rhs->roaring_bitmap);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        Data & data_lhs = this->data(place);
        const Data & data_rhs = this->data(rhs);

        if (!data_rhs.init)
            return;

        if (!data_lhs.init)
        {
            data_lhs.init = true;
            data_lhs.roaring_bitmap.merge(data_rhs.roaring_bitmap);
        }
        else
        {
            data_lhs.roaring_bitmap.rb_and(data_rhs.roaring_bitmap);
        }
    }

    bool isVersioned() const override { return false; }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).roaring_bitmap.write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).init = true;
        this->data(place).roaring_bitmap.read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        UInt64 cardinality = this->data(place).roaring_bitmap.size();

        if (cardinality > 10000000)
        {
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "There are too many returned data ({}) for the bitmap.", cardinality);
        }
        else
        {
            auto & arr_to = assert_cast<ColumnArray &>(to);
            ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
            PaddedPODArray<T> & res_data = typeid_cast<ColumnVector<T> &>(arr_to.getData()).getData();
            UInt64 count = this->data(place).roaring_bitmap.rb_to_array(res_data);
            offsets_to.push_back(offsets_to.back() + count);
        }
    }
};

void registerKeAggregateFunctionsBitmap(AggregateFunctionFactory & factory);
}
