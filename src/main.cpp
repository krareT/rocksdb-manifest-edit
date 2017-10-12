//
// Created by wiklvrain on 17-9-29.
//
#include <cstdio>
#include <string>
#include <fstream>
#include <sys/stat.h>
#include <dirent.h>

#include "rocksdb/cache.h"
#include "manifest_process.h"

const bool JSON = true;
const bool MANIFEST = false;

void print_help() {
    fprintf(stderr, "manifest-dump [OPTIONS]\n\n");
    fprintf(stderr, "%-12s %-s\n", "--json", "translate rocksdb's manifest into json type file, default");
    fprintf(stderr, "%-12s %-s\n", "--manifest", "translate json type file into rocksdb's manifest");
    fprintf(stderr, "%-12s %-s\n", "--batch", "batch translate json file into manifest or translate manifest to json");
    fprintf(stderr, "%-12s %-s\n", "--jpath", "specify json type file path");
    fprintf(stderr, "%-12s %-s\n", "--mpath", "specify manifest file path");
    fprintf(stderr, "%-12s %-s\n", "--help", "print help information");
}

rocksdb::Status process_translate(std::string& source_file, std::string& target_file, bool type) {
    std::fstream f(source_file.c_str());
    rocksdb::Status s = rocksdb::Status::OK();
    if (!f) s = rocksdb::Status::IOError("%s can not open as a file\n", source_file.c_str());
    f.close();
    if (!s.ok()) return s;
    if (type == JSON) {
        s = terark::ManifestProcess().TransToJsonFromManifest(source_file, target_file, rocksdb::Env::Default());
    } else {
        rocksdb::DBOptions db_options;
        rocksdb::ImmutableDBOptions immutable_db_options(db_options);
        rocksdb::MutableDBOptions mutableDBOptions(db_options);
        rocksdb::EnvOptions env_options(db_options);
        const int table_cache_size = (immutable_db_options.max_open_files == -1) ? 4194304 : immutable_db_options.max_open_files - 10;
        std::shared_ptr<rocksdb::Cache> cache = rocksdb::NewLRUCache(table_cache_size, immutable_db_options.table_cache_numshardbits);
        rocksdb::WriteController writeController(mutableDBOptions.delayed_write_rate);
        rocksdb::VersionSet vs(target_file,
                               &immutable_db_options, env_options,
                               cache.get(), immutable_db_options.write_buffer_manager.get(),
                               &writeController);
        s = terark::ManifestProcess().TransToManifestFromJson(target_file, source_file, rocksdb::Env::Default(), &vs);
    }
    return s;
}

rocksdb::Status batch_process_translate(std::string& source_dir, std::string& target_dir, bool type) {
    struct stat _stat;
    rocksdb::Status s;
    lstat(source_dir.c_str(), &_stat);
    if (!S_ISDIR(_stat.st_mode)) {
        fprintf(stderr, "%s is not a directory or please do not use --batch option\n", source_dir.c_str());
        exit(1);
    }
    dirent* filename;
    DIR *dir;
    dir = opendir(source_dir.c_str());
    if (dir == nullptr) {
        fprintf(stderr, "can not open directory %s, please try it again\n", source_dir.c_str());
        exit(1);
    }

    if (source_dir.at(source_dir.length() - 1) != '/') source_dir += '/';
    if (target_dir.at(target_dir.length() - 1) != '/') target_dir += '/';
    while ((filename = readdir(dir)) != nullptr) {
        if (!strcmp(filename->d_name, ".") || !strcmp(filename->d_name, ".."))
            continue;
        std::string source_file = source_dir + std::string(filename->d_name);
        std::string target_file = target_dir + std::string(filename->d_name) + ".json";
        s = process_translate(source_file, target_file, type);
        if (!s.ok()) return s;
    }
    return s;
}

int main(int argc, char** argv) {
    if (argc < 2) {
        print_help();
        exit(1);
    }

    bool type = JSON;
    bool batch = false;
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
                json_path = terark::split(command, '=')[1];
            } else if (temp == "--mpath") {
                manifest_path = terark::split(command, '=')[1];
            } else {
                fprintf(stderr, "Invalid Option: %s\n\n", command.c_str());
                print_help();
                exit(1);
            }
        } else if (command == "--batch") {
            batch = true;
        } else {
            fprintf(stderr, "Invalid Option: %s\n\n", command.c_str());
            print_help();
            exit(1);
        }
    }

    rocksdb::Status s;

    if (type == JSON) {
        if (batch) s = batch_process_translate(manifest_path, json_path, type);
        else s = process_translate(manifest_path, json_path, type);
    } else {
        if (batch) s = batch_process_translate(json_path, manifest_path, type);
        else s = process_translate(json_path, manifest_path, type);
    }
    if (s.ok()) puts("Success!");
    else fprintf(stderr, "%s\n", s.ToString().c_str());
    return 0;
}
