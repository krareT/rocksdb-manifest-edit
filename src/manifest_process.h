//
// Created by wiklvrain on 17-9-28.
//

#ifndef ROCKSDB_MANIFEST_DUMP_MANIFEST_PROCESS_H
#define ROCKSDB_MANIFEST_DUMP_MANIFEST_PROCESS_H

#include "rocksdb/status.h"
#include "db/version_set.h"
#include "db/version_edit.h"
#include "terark/terichdb/json.hpp"

namespace terark {

extern std::vector<std::string> split(std::string str, char split_char);

class ManifestProcess {
public:
    std::string getInternalKey(std::string key);
    terark::json VersionEditToJson(rocksdb::VersionEdit &edit);
    rocksdb::Status TransToJsonFromManifest(
            const std::string &manifest_path,
            const std::string &json_path,
            rocksdb::Env *env);
    rocksdb::Status TransToManifestFromJson(
            const std::string &manifest_path,
            const std::string &json_path,
            rocksdb::Env *env, rocksdb::VersionSet *version_set);
};

}

#endif //ROCKSDB_MANIFEST_DUMP_MANIFEST_PROCESS_H
