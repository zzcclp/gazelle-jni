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
#include "CustomStorageMergeTree.h"


#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Storages/MergeTree/checkDataPart.h>

namespace DB
{
namespace ErrorCodes
{
extern const int DUPLICATE_DATA_PART;
}
}

namespace local_engine
{
CustomStorageMergeTree::CustomStorageMergeTree(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    bool attach,
    ContextMutablePtr context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> storage_settings_,
    bool /*has_force_restore_data_flag*/)
    : MergeTreeData(
          table_id_,
          metadata_,
          context_,
          date_column_name,
          merging_params_,
          std::move(storage_settings_),
          false, /// require_part_metadata
          attach)
    , writer(*this)
    , reader(*this)
{
    initializeDirectoriesAndFormatVersion(relative_data_path_, attach, date_column_name);
}

std::atomic<int> CustomStorageMergeTree::part_num;

DataPartsVector CustomStorageMergeTree::loadDataPartsWithNames(std::unordered_set<std::string> parts)
{
    DataPartsVector data_parts;
    const auto disk = getStoragePolicy()->getDisks().at(0);
    for (const auto& name : parts)
    {
        part_num ++;
        MergeTreePartInfo part_info = {"all", part_num, part_num, 0};
        auto res = loadDataPart(part_info, name, disk, MergeTreeDataPartState::Active);
        data_parts.emplace_back(res.part);
    }
    return data_parts;
}

MergeTreeData::LoadPartResult CustomStorageMergeTree::loadDataPart(
    const MergeTreePartInfo & part_info,
    const String & part_name,
    const DiskPtr & part_disk_ptr,
    MergeTreeDataPartState to_state)
{
    LOG_TRACE(log, "Loading {} part {} from disk {}", magic_enum::enum_name(to_state), part_name, part_disk_ptr->getName());

    LoadPartResult res;
    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, part_disk_ptr, 0);
    auto data_part_storage = std::make_shared<DataPartStorageOnDiskFull>(single_disk_volume, relative_data_path, part_name);

    String part_path = fs::path(relative_data_path) / part_name;


    try
    {
        res.part = getDataPartBuilder(part_name, single_disk_volume, part_name).withPartInfo(part_info).withPartFormatFromDisk().build();
    }
    catch (...)
    {
        /// Don't count the part as broken if there was a retryalbe error
        /// during loading, such as "not enough memory" or network error.
        if (isRetryableException(std::current_exception()))
            throw;
        LOG_DEBUG(log, "Failed to load data part {}, unknown exception", part_name);
        return res;
    }

    try
    {
        res.part->loadColumnsChecksumsIndexes(require_part_metadata, true);
    }
    catch (...)
    {
        /// Don't count the part as broken if there was a retryalbe error
        /// during loading, such as "not enough memory" or network error.
        if (isRetryableException(std::current_exception()))
            throw;
        return res;
    }

    res.part->modification_time = part_disk_ptr->getLastModified(fs::path(relative_data_path) / part_name).epochTime();
    res.part->loadVersionMetadata();

    res.part->setState(to_state);

    DataPartIteratorByInfo it;
    bool inserted;

    {
        LOG_TEST(log, "loadDataPart: inserting {} into data_parts_indexes", res.part->getNameWithState());
        std::tie(it, inserted) = data_parts_indexes.insert(res.part);
    }

    /// Remove duplicate parts with the same checksum.
    if (!inserted)
    {
        if ((*it)->checksums.getTotalChecksumHex() == res.part->checksums.getTotalChecksumHex())
        {
            LOG_ERROR(log, "Remove duplicate part {}", data_part_storage->getFullPath());
            res.part->is_duplicate = true;
            return res;
        }
        else
            throw Exception(ErrorCodes::DUPLICATE_DATA_PART, "Part {} already exists but with different checksums", res.part->name);
    }

    // if (to_state == DataPartState::Active)
    //     addPartContributionToDataVolume(res.part);

    if (res.part->hasLightweightDelete())
        has_lightweight_delete_parts.store(true);

    LOG_TRACE(log, "Finished loading {} part {} on disk {}", magic_enum::enum_name(to_state), part_name, part_disk_ptr->getName());
    return res;
}

void CustomStorageMergeTree::dropPartNoWaitNoThrow(const String & /*part_name*/)
{
    throw std::runtime_error("not implement");
}
void CustomStorageMergeTree::dropPart(const String & /*part_name*/, bool /*detach*/, ContextPtr /*context*/)
{
    throw std::runtime_error("not implement");
}
void CustomStorageMergeTree::dropPartition(const ASTPtr & /*partition*/, bool /*detach*/, ContextPtr /*context*/)
{
}
PartitionCommandsResultInfo CustomStorageMergeTree::attachPartition(
    const ASTPtr & /*partition*/, const StorageMetadataPtr & /*metadata_snapshot*/, bool /*part*/, ContextPtr /*context*/)
{
    throw std::runtime_error("not implement");
}
void CustomStorageMergeTree::replacePartitionFrom(
    const StoragePtr & /*source_table*/, const ASTPtr & /*partition*/, bool /*replace*/, ContextPtr /*context*/)
{
    throw std::runtime_error("not implement");
}
void CustomStorageMergeTree::movePartitionToTable(const StoragePtr & /*dest_table*/, const ASTPtr & /*partition*/, ContextPtr /*context*/)
{
    throw std::runtime_error("not implement");
}
bool CustomStorageMergeTree::partIsAssignedToBackgroundOperation(const MergeTreeData::DataPartPtr & /*part*/) const
{
    throw std::runtime_error("not implement");
}

std::string CustomStorageMergeTree::getName() const
{
    throw std::runtime_error("not implement");
}
std::vector<MergeTreeMutationStatus> CustomStorageMergeTree::getMutationsStatus() const
{
    throw std::runtime_error("not implement");
}
bool CustomStorageMergeTree::scheduleDataProcessingJob(BackgroundJobsAssignee & /*executor*/)
{
    throw std::runtime_error("not implement");
}
void CustomStorageMergeTree::startBackgroundMovesIfNeeded()
{
    throw std::runtime_error("not implement");
}
std::unique_ptr<MergeTreeSettings> CustomStorageMergeTree::getDefaultSettings() const
{
    throw std::runtime_error("not implement");
}
std::map<std::string, MutationCommands> CustomStorageMergeTree::getUnfinishedMutationCommands() const
{
    throw std::runtime_error("not implement");
}
}
