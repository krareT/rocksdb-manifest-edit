//
// Created by wiklvrain on 17-9-29.
//
#include <cstdio>
#include <string>
#include <fstream>

#include "rocksdb/cache.h"
#include "manifest_process.h"

const bool JSON = true;
const bool MANIFEST = false;

void print_help() {
    fprintf(stderr, "manifest-dump [OPTIONS]\n\n");
    fprintf(stderr, "%-12s %-s\n", "--json", "translate rocksdb's manifest into json type file, default");
    fprintf(stderr, "%-12s %-s\n", "--manifest", "translate json type file into rocksdb's manifest");
    fprintf(stderr, "%-12s %-s\n", "--jpath", "specify json type file path");
    fprintf(stderr, "%-12s %-s\n", "--mpath", "specify manifest file path");
    fprintf(stderr, "%-12s %-s\n", "--help", "print help information");
}

std::vector<std::string> split(std::string str, char split_char) {
  std::string s = "";
  std::vector<std::string> split_str;
  split_str.clear();
  for (decltype(str.length()) i = 0; i < str.length(); ++i) {
    if (str[i] != split_char) s += str[i];
    if (str[i] == split_char || (i + 1) == str.length()) {
      split_str.emplace_back(s);
      s = "";
    }
  }
  return split_str;
};

int main(int argc, char** argv) {
    if (argc < 2) {
        print_help();
        exit(1);
    }

    bool type = JSON;
    std::string json_path = "";
    std::string manifest_path = "";

    for (int i = 1; i < argc; ++i) {
        std::string command = argv[i];
        if (command == "--json") {
            type = JSON;
        } else if (command == "--manifest") {
            type = MANIFEST;
        } else if (command == "--help") {
            print_help();
            exit(0);
        } else if (command.length() > 7) {
            std::string temp = command.substr(0, 7);
            if (temp == "--jpath") {
                json_path = split(command, '=')[1];
            } else if (temp == "--mpath") {
                manifest_path = split(command, '=')[1];
            } else {
                fprintf(stderr, "Invalid Option: %s\n\n", command.c_str());
                print_help();
                exit(1);
            }
        } else {
            fprintf(stderr, "Invalid Option: %s\n\n", command.c_str());
            print_help();
            exit(1);
        }
    }

    rocksdb::Status s;

    if (type == JSON) {
        std::fstream f(manifest_path);
        if (!f) {
            fprintf(stderr, "%s can not open as a file", json_path.c_str());
            f.close();
            exit(1);
        }
        f.close();
        s = terark::ManifestProcess().TransToJsonFromManifest(manifest_path, json_path, rocksdb::Env::Default());
    } else {
        std::fstream f(json_path);
        if (!f) {
            fprintf(stderr, "%s can not open as a file", json_path.c_str());
            f.close();
            exit(1);
        }
        f.close();
        rocksdb::DBOptions db_options;
        rocksdb::ImmutableDBOptions immutable_db_options(db_options);
        rocksdb::MutableDBOptions mutableDBOptions(db_options);
        rocksdb::EnvOptions env_options(db_options);
        const int table_cache_size = (immutable_db_options.max_open_files == -1) ? 4194304 : immutable_db_options.max_open_files - 10;
        std::shared_ptr<rocksdb::Cache> cache = rocksdb::NewLRUCache(table_cache_size, immutable_db_options.table_cache_numshardbits);
        rocksdb::WriteController writeController(mutableDBOptions.delayed_write_rate);
        rocksdb::VersionSet vs(manifest_path, &immutable_db_options,
                               env_options, cache.get(),
                               immutable_db_options.write_buffer_manager.get(),
                               &writeController);
        s = terark::ManifestProcess().TransToManifestFromJson(manifest_path, json_path, rocksdb::Env::Default(), &vs);
    }
    if (s.ok()) printf("Success!");
    else fprintf(stderr, "%s\n", s.ToString().c_str());
    return 0;
}