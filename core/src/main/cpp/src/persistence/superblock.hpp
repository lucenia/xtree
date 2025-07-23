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

#pragma once
#include <cstdint>
#include <string>
#include "node_id.hpp"
#include "platform_fs.h"

namespace xtree { namespace persist {

        // Ensure stable layout across compilers for checksumming
        #pragma pack(push, 1)
        struct SuperblockOnDisk {
            uint64_t magic;                     // 0
            uint32_t version;                   // 8
            uint32_t header_size;               // 12

            uint32_t seq;                       // 16 - Seqlock counter for torn read prevention
            uint32_t _pad32_seq;                // 20 - padding to align next u64 at 24

            uint64_t root_id;                   // 24 - NodeID (raw)
            uint64_t commit_epoch;              // 32 - MVCC

            uint64_t generation;                // 40
            uint64_t created_unix;              // 48

            uint32_t header_crc32c;             // 56 - CRC32C of header
            uint32_t _pad32_crc;                // 60 - padding to align following region to 64 bytes

            uint8_t  pad[256];                  // 64...319 future (back to 256 after adding seq)
        };
        #pragma pack(pop)

        // Layout/align guarantees (fail fast if a compiler changes anything)
        static_assert(sizeof(SuperblockOnDisk) == 320, "SuperblockOnDisk size must be stable");
        static_assert(offsetof(SuperblockOnDisk, magic) == 0, "magic offset mismatch");
        static_assert(offsetof(SuperblockOnDisk, version) == 8, "version offset mismatch");
        static_assert(offsetof(SuperblockOnDisk, header_size) == 12, "header_size offset mismatch");
        static_assert(offsetof(SuperblockOnDisk, seq) == 16, "seq offset mismatch");
        static_assert(offsetof(SuperblockOnDisk, root_id) == 24, "root_id offset mismatch");
        static_assert(offsetof(SuperblockOnDisk, commit_epoch) == 32, "commit_epoch offset mismatch");
        static_assert(offsetof(SuperblockOnDisk, generation) == 40, "generation offset mismatch");
        static_assert(offsetof(SuperblockOnDisk, created_unix) == 48, "created_unix offset mismatch");
        static_assert(offsetof(SuperblockOnDisk, header_crc32c) == 56, "header_crc32c offset mismatch");

        class Superblock {
        public:
            static constexpr uint64_t SUPERBLOCK_MAGIC = 0x5854524545424C4B; // "XTREEBLK"
            static constexpr uint32_t SUPERBLOCK_VERSION = 1;
            
            explicit Superblock(const std::string& meta_path);
            ~Superblock();

            struct Snapshot { 
                NodeID root; 
                uint64_t epoch; 
            };

            Snapshot load() const;                      // atomic acquire
            void     publish(NodeID new_root, uint64_t new_epoch); // with fsync ordering
            inline bool     valid() const { return sb_ && sb_->magic == SUPERBLOCK_MAGIC; }
        private:
            std::string path_;
            SuperblockOnDisk* sb_ = nullptr; // mapped
            MappedRegion region_{};           // keep the mapping
        };

    } // namespace persist
} // namespace xtree::persist