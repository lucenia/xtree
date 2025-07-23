/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * The Lucenia project is free software: you can redistribute it
 * and/or modify it under the terms of the GNU Affero General
 * Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, see:
 * https://www.gnu.org/licenses/agpl-3.0.html
 */

#include "manifest.h"
#include "platform_fs.h"
#include "../util/log.h"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/error/en.h"
#include <fstream>
#include <sstream>
#include <iomanip>
#include <filesystem>
#include <algorithm>
#include <iostream>
#include <cstdio>
#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#else
#include <windows.h>
#endif

namespace xtree { 
namespace persist {

namespace fs = std::filesystem;

Manifest::Manifest(const std::string& data_dir) 
    : data_dir_(data_dir) {
    created_unix_ = std::time(nullptr);
}

std::string Manifest::get_manifest_path() const {
    std::filesystem::path p(data_dir_);
    p /= "manifest.json";
    return p.string();
}

bool Manifest::load() {
    std::string manifest_path = get_manifest_path();
    
    // Read entire file (binary mode for consistency)
    std::ifstream file(manifest_path, std::ios::binary);
    if (!file.is_open()) {
        return false;
    }
    
    std::stringstream buffer;
    buffer << file.rdbuf();
    file.close();
    
    std::string json_str = buffer.str();
    if (json_str.empty()) {
        return false;
    }
    
    return from_json(json_str);
}

bool Manifest::reload() {
    // Simply clear current state and reload from disk
    checkpoint_ = {};
    delta_logs_.clear();
    data_files_.clear();
    return load();
}

bool Manifest::store() {
    // Ensure directory exists
    std::error_code ec;
    std::filesystem::create_directories(data_dir_, ec);
    if (ec) {
        return false;
    }
    
    std::string manifest_path = get_manifest_path();
    std::string temp_path = manifest_path + ".tmp";
    
    // Serialize to JSON
    std::string json_str = to_json();
    
    // Write to temp file (binary mode for cross-platform consistency)
    std::ofstream file(temp_path, std::ios::trunc | std::ios::binary);
    if (!file.is_open()) {
        return false;
    }
    
    file << json_str;
    file.flush();
    file.close();
    
    // Sync temp file
    #ifndef _WIN32
        int fd = ::open(temp_path.c_str(), O_RDWR);
        if (fd >= 0) {
            ::fsync(fd);
            ::close(fd);
        }
    #else
        HANDLE hFile = CreateFileA(temp_path.c_str(), 
                                 GENERIC_WRITE,
                                 FILE_SHARE_READ | FILE_SHARE_WRITE,
                                 NULL,
                                 OPEN_EXISTING,
                                 FILE_ATTRIBUTE_NORMAL,
                                 NULL);
        if (hFile != INVALID_HANDLE_VALUE) {
            FlushFileBuffers(hFile);
            CloseHandle(hFile);
        }
    #endif
    
    // Atomic rename
    FSResult rename_res = PlatformFS::atomic_replace(temp_path, manifest_path);
    if (!rename_res.ok) {
        std::remove(temp_path.c_str());
        return false;
    }
    
    // Fsync directory
    FSResult fsync_res = PlatformFS::fsync_directory(data_dir_);
    if (!fsync_res.ok) {
        return false;
    }
    
    return true;
}

void Manifest::prune_old_delta_logs(uint64_t checkpoint_epoch) {
    // Remove delta logs that are entirely before checkpoint
    delta_logs_.erase(
        std::remove_if(delta_logs_.begin(), delta_logs_.end(),
            [checkpoint_epoch](const DeltaLogInfo& log) {
                return log.end_epoch != 0 && log.end_epoch <= checkpoint_epoch;
            }),
        delta_logs_.end()
    );
}

std::vector<Manifest::DeltaLogInfo> Manifest::get_logs_after_checkpoint(uint64_t checkpoint_epoch) const {
    std::vector<DeltaLogInfo> result;
    for (const auto& log : delta_logs_) {
        // Only include logs that start after the checkpoint epoch
        if (log.start_epoch > checkpoint_epoch) {
            result.push_back(log);
        }
    }
    return result;
}

std::string Manifest::to_json() const {
    rapidjson::StringBuffer buffer;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
    writer.SetIndent(' ', 2);
    
    writer.StartObject();
    
    writer.Key("version");
    writer.Uint(version_);
    
    writer.Key("created_unix");
    writer.Int64(created_unix_);
    
    writer.Key("superblock");
    writer.String(superblock_path_.c_str());
    
    // Checkpoint info
    writer.Key("checkpoint");
    writer.StartObject();
    if (!checkpoint_.path.empty()) {
        writer.Key("path");
        writer.String(checkpoint_.path.c_str());
        writer.Key("epoch");
        writer.Uint64(checkpoint_.epoch);
        writer.Key("size");
        writer.Uint64(checkpoint_.size);
        writer.Key("entries");
        writer.Uint64(checkpoint_.entries);
        writer.Key("crc32c");
        // Format as hex string
        char hex_buf[16];
        snprintf(hex_buf, sizeof(hex_buf), "0x%08x", checkpoint_.crc32c);
        writer.String(hex_buf);
    }
    writer.EndObject();
    
    // Delta logs
    writer.Key("delta_logs");
    writer.StartArray();
    for (const auto& log : delta_logs_) {
        writer.StartObject();
        writer.Key("path");
        writer.String(log.path.c_str());
        writer.Key("start_epoch");
        writer.Uint64(log.start_epoch);
        writer.Key("end_epoch");
        if (log.end_epoch == 0) {
            writer.Null();
        } else {
            writer.Uint64(log.end_epoch);
        }
        writer.Key("size");
        writer.Uint64(log.size);
        writer.EndObject();
    }
    writer.EndArray();
    
    // Data files
    writer.Key("data_files");
    writer.StartArray();
    for (const auto& df : data_files_) {
        writer.StartObject();
        writer.Key("class");
        writer.Uint(df.class_id);
        writer.Key("seq");
        writer.Uint(df.seq);
        writer.Key("file");
        writer.String(df.file.c_str());
        writer.Key("bytes");
        writer.Uint64(df.bytes);
        writer.EndObject();
    }
    writer.EndArray();
    
    // Root catalog - always include if present
    if (!roots_.empty()) {
        writer.Key("roots");
        writer.StartArray();
        for (const auto& root : roots_) {
            writer.StartObject();
            writer.Key("name");
            writer.String(root.name.c_str());
            writer.Key("node_id");
            // Write as hex string for readability
            char hex_buf[32];
            snprintf(hex_buf, sizeof(hex_buf), "0x%016llx", 
                    static_cast<unsigned long long>(root.node_id_raw));
            writer.String(hex_buf);
            writer.Key("epoch");
            writer.Uint64(root.epoch);
            
            // Write MBR if present
            if (!root.mbr.empty()) {
                writer.Key("mbr");
                writer.StartArray();
                for (float val : root.mbr) {
                    writer.Double(val);  // JSON doesn't have float, use double
                }
                writer.EndArray();
            }
            
            writer.EndObject();
        }
        writer.EndArray();
    }
    
    writer.EndObject();
    
    return buffer.GetString();
}

bool Manifest::from_json(const std::string& json_str) {
    rapidjson::Document doc;
    doc.Parse(json_str.c_str());
    
    // Check for parse errors
    if (doc.HasParseError()) {
        error() << "JSON parse error at offset " << doc.GetErrorOffset() 
                << ": " << rapidjson::GetParseError_En(doc.GetParseError());
        return false;
    }
    
    // Validate it's an object
    if (!doc.IsObject()) {
        return false;
    }
    
    // Parse version
    if (doc.HasMember("version") && doc["version"].IsUint()) {
        version_ = doc["version"].GetUint();
    }
    
    // Parse created_unix
    if (doc.HasMember("created_unix") && doc["created_unix"].IsInt64()) {
        created_unix_ = doc["created_unix"].GetInt64();
    }
    
    // Parse superblock path
    if (doc.HasMember("superblock") && doc["superblock"].IsString()) {
        superblock_path_ = doc["superblock"].GetString();
    }
    
    // Parse checkpoint
    if (doc.HasMember("checkpoint") && doc["checkpoint"].IsObject()) {
        const auto& ckpt = doc["checkpoint"];
        
        if (ckpt.HasMember("path") && ckpt["path"].IsString()) {
            checkpoint_.path = ckpt["path"].GetString();
        }
        if (ckpt.HasMember("epoch") && ckpt["epoch"].IsUint64()) {
            checkpoint_.epoch = ckpt["epoch"].GetUint64();
        }
        if (ckpt.HasMember("size") && ckpt["size"].IsUint64()) {
            checkpoint_.size = ckpt["size"].GetUint64();
        }
        if (ckpt.HasMember("entries") && ckpt["entries"].IsUint64()) {
            checkpoint_.entries = ckpt["entries"].GetUint64();
        }
        if (ckpt.HasMember("crc32c") && ckpt["crc32c"].IsString()) {
            // Parse hex string
            const char* hex_str = ckpt["crc32c"].GetString();
            checkpoint_.crc32c = std::strtoul(hex_str, nullptr, 16);
        }
    }
    
    // Parse delta logs
    delta_logs_.clear();
    if (doc.HasMember("delta_logs") && doc["delta_logs"].IsArray()) {
        const auto& logs = doc["delta_logs"];
        for (rapidjson::SizeType i = 0; i < logs.Size(); i++) {
            if (!logs[i].IsObject()) continue;
            
            const auto& log_obj = logs[i];
            DeltaLogInfo log;
            
            if (log_obj.HasMember("path") && log_obj["path"].IsString()) {
                log.path = log_obj["path"].GetString();
            }
            if (log_obj.HasMember("start_epoch") && log_obj["start_epoch"].IsUint64()) {
                log.start_epoch = log_obj["start_epoch"].GetUint64();
            }
            if (log_obj.HasMember("end_epoch")) {
                if (log_obj["end_epoch"].IsNull()) {
                    log.end_epoch = 0;
                } else if (log_obj["end_epoch"].IsUint64()) {
                    log.end_epoch = log_obj["end_epoch"].GetUint64();
                }
            }
            if (log_obj.HasMember("size") && log_obj["size"].IsUint64()) {
                log.size = log_obj["size"].GetUint64();
            }
            
            delta_logs_.push_back(log);
        }
    }
    
    // Parse data files
    data_files_.clear();
    if (doc.HasMember("data_files") && doc["data_files"].IsArray()) {
        const auto& files = doc["data_files"];
        for (rapidjson::SizeType i = 0; i < files.Size(); i++) {
            if (!files[i].IsObject()) continue;
            
            const auto& file_obj = files[i];
            DataFileInfo df;
            
            if (file_obj.HasMember("class") && file_obj["class"].IsUint()) {
                df.class_id = static_cast<uint8_t>(file_obj["class"].GetUint());
            }
            if (file_obj.HasMember("seq") && file_obj["seq"].IsUint()) {
                df.seq = file_obj["seq"].GetUint();
            }
            if (file_obj.HasMember("file") && file_obj["file"].IsString()) {
                df.file = file_obj["file"].GetString();
            }
            if (file_obj.HasMember("bytes") && file_obj["bytes"].IsUint64()) {
                df.bytes = file_obj["bytes"].GetUint64();
            }
            
            data_files_.push_back(df);
        }
    }
    
    // Parse root catalog
    if (doc.HasMember("roots") && doc["roots"].IsArray()) {
        const auto& roots = doc["roots"];
        roots_.clear();
        
        for (rapidjson::SizeType i = 0; i < roots.Size(); i++) {
            if (!roots[i].IsObject()) continue;
            
            const auto& root_obj = roots[i];
            RootEntry entry;
            
            if (root_obj.HasMember("name") && root_obj["name"].IsString()) {
                entry.name = root_obj["name"].GetString();
            }
            
            if (root_obj.HasMember("node_id")) {
                if (root_obj["node_id"].IsString()) {
                    // Parse hex string
                    const char* hex_str = root_obj["node_id"].GetString();
                    entry.node_id_raw = std::strtoull(hex_str, nullptr, 0);
                } else if (root_obj["node_id"].IsUint64()) {
                    entry.node_id_raw = root_obj["node_id"].GetUint64();
                }
            }
            
            if (root_obj.HasMember("epoch") && root_obj["epoch"].IsUint64()) {
                entry.epoch = root_obj["epoch"].GetUint64();
            }
            
            // Parse MBR if present
            if (root_obj.HasMember("mbr") && root_obj["mbr"].IsArray()) {
                const auto& mbr_array = root_obj["mbr"];
                entry.mbr.reserve(mbr_array.Size());
                for (rapidjson::SizeType j = 0; j < mbr_array.Size(); j++) {
                    if (mbr_array[j].IsNumber()) {
                        entry.mbr.push_back(static_cast<float>(mbr_array[j].GetDouble()));
                    }
                }
            }
            
            roots_.push_back(entry);
        }
    }
    
    return true;
}

} // namespace persist
} // namespace xtree