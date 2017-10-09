//
// Created by wiklvrain on 17-9-28.
//

#include "db/log_writer.h"
#include "manifest_process.h"

#include <fstream>

namespace terark {

terark::json ManifestProcess::VersionEditToJson(rocksdb::VersionEdit &edit) {
    terark::json j;
    if (edit.is_has_comparator()) j["kComparator"] = edit.get_comparator();
    if (edit.is_has_log_number()) j["kLogNumber"] = edit.get_log_number();
    if (edit.is_has_prev_log_number()) j["kPrevFileNumber"] = edit.get_prev_log_number();
    if (edit.is_has_next_file_number()) j["kNextFileNumber"] = edit.get_next_file_number();
    if (edit.is_has_last_sequence()) j["kLastSequence"] = edit.get_last_sequence();
    if (edit.is_has_max_column_family()) j["kMaxColumnFamily"] = edit.get_max_column_family();
    if (edit.is_column_family_drop()) j["kColumnFamilyDrop"] = edit.get_column_family_name();
    if (edit.is_column_family_add()) j["kColumnFamilyAdd"] = edit.get_column_family_name();
    j["kColumnFamily"] = edit.get_column_family();

    terark::json delete_files = terark::json::array();
    rocksdb::VersionEdit::DeletedFileSet dfs = edit.get_deleted_files();
    if (dfs.size() > 0) {
        for (auto &df: dfs) {
            terark::json tmp;
            tmp["level"] = df.first;
            tmp["file_number"] = df.second;
            delete_files.push_back(tmp);
        }
    }
    j["kDeletedFile"] = delete_files;

    terark::json add_files = terark::json::array();
    std::vector<std::pair<int, rocksdb::FileMetaData>> afs = edit.get_new_files();
    if (afs.size() > 0) {
        for (auto &af: afs) {
            terark::json tmp;
            tmp["level"] = af.first;
            const rocksdb::FileMetaData &f = af.second;
            tmp["file_number"] = f.fd.GetNumber();
            tmp["file_size"] = f.fd.GetFileSize();
            tmp["path_id"] = f.fd.GetPathId();
            rocksdb::InternalKey smallest_key = f.smallest, largest_key = f.largest;
            tmp["min_seqno"] = f.smallest_seqno;
            tmp["max_seqno"] = f.largest_seqno;
            tmp["min_key_hex"] = smallest_key.DebugString(true);
            tmp["max_key_hex"] = largest_key.DebugString(true);
            tmp["kNeedCompaction"] = f.marked_for_compaction;
            add_files.push_back(tmp);
        }
    }
    j["AddedFiles"] = add_files;
    return j;
}

std::string ManifestProcess::getInternalKey(std::string key) {
    auto split = [](std::string str, char split_char) {
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
    auto toDec = [](char num) {
        if (num >= 'A') return static_cast<int>(num - 'A' + 10);
        else return static_cast<int>(num - '0');
    };
    std::vector<std::string> splited_string = split(key, ' ');
    assert(splited_string.size() == 4);
    std::string result = "";
    int type = 0;
    rocksdb::SequenceNumber seq = 0;
    for (decltype(splited_string.size()) i = 0; i < splited_string.size(); ++i) {
        if (i == 0) {
            std::string &user_key = splited_string[i];
            for (decltype(user_key.length()) j = 1; j < user_key.length() - 1; j += 2) {
                int num = toDec(user_key[j]) << 4 | toDec(user_key[j + 1]);
                result.push_back(static_cast<char>(num));
            }
        } else if (i == 1) {
            continue;
        } else if (i == 2) {
            std::string &sequence = splited_string[i];
            for (decltype(sequence.length()) j = 0; j < sequence.length() - 1; ++j)
                seq = seq * 10 + static_cast<int>(sequence[j] - '0');
        } else if (i == 3) {
            std::string &_type = splited_string[i];
            for (decltype(_type.length()) j = 0; j < _type.length(); ++j)
                type = type * 10 + static_cast<int>(_type[j] - '0');
        }
    }
    seq = seq << 8 | type;
    int cur = 7;
    std::string temp(8, '\0');
    while (seq) {
        temp[cur--] = static_cast<char>(seq & 0xff);
        seq >>= 8;
    }
    reverse(temp.begin(), temp.end());
    result += temp;
    return result;
}

rocksdb::Status ManifestProcess::TransToJsonFromManifest(
        const std::string &manifest_path,
        const std::string &json_path,
        rocksdb::Env *env) {
    rocksdb::EnvOptions soptions;
    rocksdb::Status s;
    std::unique_ptr <rocksdb::SequentialFileReader> file_reader;
    {
        std::unique_ptr <rocksdb::SequentialFile> file;
        s = env->NewSequentialFile(manifest_path, &file, soptions);
        if (!s.ok()) return s;
        file_reader.reset(new rocksdb::SequentialFileReader(std::move(file)));
    }

    struct LogReporter : public rocksdb::log::Reader::Reporter {
        rocksdb::Status* status;
        virtual void Corruption(size_t bytes, const rocksdb::Status& s) override {
            if (this->status->ok()) *this->status = s;
        }
    };
    LogReporter reporter;
    reporter.status = &s;
    rocksdb::log::Reader reader(NULL, std::move(file_reader), &reporter, true, 0, 0);

    rocksdb::Slice record;
    std::string scratch;
    terark::json version_json = terark::json::array();
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
        rocksdb::VersionEdit edit;
        s = edit.DecodeFrom(record);
        if (!s.ok()) break;
        version_json.push_back(VersionEditToJson(edit));
    }

    std::ofstream out(json_path, std::ios::out);
    out << "\xEF\xBB\xBF";
    out << version_json.dump(2);
    out.close();
    return s;
}

rocksdb::Status ManifestProcess::TransToManifestFromJson(
        const std::string &manifest_path,
        const std::string &json_path,
        rocksdb::Env *env,
        rocksdb::VersionSet *version_set) {
    rocksdb::EnvOptions soptions;
    rocksdb::Status s;
    std::string _json = "", json_tmp = "";
    std::ifstream in(json_path, std::ios::in);

    in.seekg(3, std::ios::beg);
    while (std::getline(in, json_tmp))
        _json += json_tmp;
    terark::json j = terark::json::parse(_json);

    std::unique_ptr <rocksdb::WritableFile> descriptor_file;
    rocksdb::EnvOptions opt_env_opts = version_set->get_env()->OptimizeForManifestWrite(version_set->env_options());
    s = NewWritableFile(
            version_set->get_env(),
            manifest_path,
            &descriptor_file, opt_env_opts);
    if (s.ok()) {
        descriptor_file->SetPreallocationBlockSize(version_set->get_db_options()->manifest_preallocation_size);

        std::unique_ptr <rocksdb::WritableFileWriter> file_writer(
                new rocksdb::WritableFileWriter(std::move(descriptor_file), opt_env_opts));
        version_set->get_descriptor_log().reset(new rocksdb::log::Writer(std::move(file_writer), 0, false));
    }

    for (auto it = j.begin(); it != j.end(); ++it) {
        rocksdb::VersionEdit edit;
        terark::json version_edit = it.value();
        for (auto vit = version_edit.begin(); vit != version_edit.end(); ++vit) {
            std::string key = vit.key();
            auto val = vit.value();
            if (key == "kComparator") {
                if (val.type() == terark::json::value_t::string)
                    edit.SetComparatorName(val.get<std::string>());
                else s = rocksdb::Status::Corruption("Wrong type value.");
            } else if (key == "kLogNumber") {
                if (val.type() == terark::json::value_t::number_unsigned)
                    edit.SetLogNumber(val.get<uint64_t>());
                else s = rocksdb::Status::Corruption("Wrong type value.");
            } else if (key == "kPrevFileNumber") {
                if (val.type() == terark::json::value_t::number_unsigned)
                    edit.SetPrevLogNumber(val.get<uint64_t>());
                else s = rocksdb::Status::Corruption("Wrong type value.");
            } else if (key == "kNextFileNumber") {
                if (val.type() == terark::json::value_t::number_unsigned)
                    edit.SetNextFile(val.get<uint64_t>());
                else s = rocksdb::Status::Corruption("Wrong type value.");
            } else if (key == "kLastSequence") {
                if (val.type() == terark::json::value_t::number_unsigned)
                    edit.SetLastSequence(val.get<uint64_t>());
                else s = rocksdb::Status::Corruption("Wrong type value.");
            } else if (key == "kMaxColumnFamily") {
                if (val.type() == terark::json::value_t::number_unsigned)
                    edit.SetMaxColumnFamily(val.get<uint32_t>());
                else s = rocksdb::Status::Corruption("Wrong type value.");
            } else if (key == "kDeletedFile") {
                terark::json delete_files = val;
                if (val.type() != terark::json::value_t::array) {
                    s = rocksdb::Status::Corruption("Wrong type value.");
                    continue;
                }
                int first;
                uint64_t second;
                for (decltype(delete_files.size()) i = 0; i < delete_files.size(); ++i) {
                    terark::json tmp = delete_files[i];
                    for (auto df_it = tmp.begin(); df_it != tmp.end(); ++df_it) {
                        if (df_it.key() == "level") first = df_it.value().get<int>();
                        else if (df_it.key() == "file_number") second = df_it.value().get<uint64_t>();
                        else s = rocksdb::Status::Corruption("Json key " + df_it.key() + " do not match any key.");
                    }
                    edit.DeleteFile(first, second);
                }
            } else if (key == "AddedFiles") {
                terark::json add_files = val;
                for (decltype(add_files.size()) i = 0; i < add_files.size(); ++i) {
                    int first = 0;
                    uint32_t path_id = 0;
                    bool need_compaction = false;
                    uint64_t number = 0, file_size = 0;
                    rocksdb::InternalKey smallest_key, largest_key;
                    rocksdb::SequenceNumber smallest_seqno = 0, largest_seqno = 0;
                    for (terark::json::iterator af_it = add_files[i].begin(); af_it != add_files[i].end(); ++af_it) {
                        auto aval = af_it.value();
                        std::string akey = af_it.key();
                        if (akey == "level") {
                            if (aval.type() == terark::json::value_t::number_unsigned)
                                first = aval.get<int>();
                            else s = rocksdb::Status::Corruption("Wrong type value.");
                        } else if (akey == "file_number") {
                            if (aval.type() == terark::json::value_t::number_unsigned)
                                number = aval.get<uint64_t>();
                            else s = rocksdb::Status::Corruption("Wrong type value.");
                        } else if (akey == "file_size") {
                            if (aval.type() == terark::json::value_t::number_unsigned)
                                file_size = aval.get<uint64_t>();
                            else s = rocksdb::Status::Corruption("Wrong type value.");
                        } else if (akey == "path_id") {
                            if (aval.type() == terark::json::value_t::number_unsigned)
                                path_id = aval.get<uint32_t>();
                            else s = rocksdb::Status::Corruption("Wrong type value.");
                        } else if (akey == "min_key_hex") {
                            if (aval.type() == terark::json::value_t::string) {
                                smallest_key.DecodeFrom(rocksdb::Slice(getInternalKey(aval.get<std::string>())));
                            } else s = rocksdb::Status::Corruption("Wrong type value.");
                        } else if (akey == "max_key_hex") {
                            if (aval.type() == terark::json::value_t::string)
                                largest_key.DecodeFrom(rocksdb::Slice(getInternalKey(aval.get<std::string>())));
                            else s = rocksdb::Status::Corruption("Wrong type value.");
                        } else if (akey == "min_seqno") {
                            if (aval.type() == terark::json::value_t::number_unsigned)
                                smallest_seqno = aval;
                            else s = rocksdb::Status::Corruption("Wrong type value.");
                        } else if (akey == "max_seqno") {
                            if (aval.type() == terark::json::value_t::number_unsigned)
                                largest_seqno = aval;
                            else s = rocksdb::Status::Corruption("Wrong type value.");
                        } else if (akey == "kNeedCompaction") {
                            if (aval.type() == terark::json::value_t::boolean)
                                need_compaction = aval;
                            else s = rocksdb::Status::Corruption("Wrong type value.");
                        } else s = rocksdb::Status::Corruption("Json key " + akey + " do not match any key.");
                    }
                    rocksdb::FileMetaData f;
                    f.largest = largest_key;
                    f.smallest = smallest_key;
                    f.largest_seqno = largest_seqno;
                    f.smallest_seqno = smallest_seqno;
                    f.marked_for_compaction = need_compaction;
                    f.fd.file_size = file_size;
                    f.fd.packed_number_and_path_id = rocksdb::PackFileNumberAndPathId(number, path_id);
                    edit.AddFile(first, f);
                }
            } else if (key == "kColumnFamily") {
                if (val.type() == terark::json::value_t::number_unsigned)
                    edit.SetColumnFamily(val.get<uint32_t>());
                else s = rocksdb::Status::Corruption("Wrong type value.");
            } else if (key == "kColumnFamilyAdd") {
                if (val.type() == terark::json::value_t::string) {
                    edit.AddColumnFamily(val.get<std::string>());
                } else s = rocksdb::Status::Corruption("Wrong type value.");
            } else if (key == "kColumnFamilyDrop") {
                if (val.type() == terark::json::value_t::string) {
                    edit.DropColumnFamily();
                } else s = rocksdb::Status::Corruption("Wrong type value.");
            } else s = rocksdb::Status::Corruption("Json key " + key + " do not match any key.");
        }
        std::string tmp = "";
        if (edit.EncodeTo(&tmp)) {
            s = version_set->get_descriptor_log()->AddRecord(tmp);
        } else s = rocksdb::Status::Corruption("VersionEdit encode to string failure!");
        if (!s.ok()) break;
    }

    if (s.ok()) s = SyncManifest(env, version_set->get_db_options(), version_set->get_descriptor_log()->file());
    in.close();
    return s;
}
}