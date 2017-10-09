# rocksdb-manifest-edit

## Note

这个项目用于读取指定的manifest文件，生成其json格式的文件到指定位置

同时，也可以读取指定的json文件，生成其对应的manifest文件

## 使用

```shell
git clone https://github.com/terark/rocksdb-manifest-edit.git
cd rocksdb-manifest-edit
git submodule init
git submodule update
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . --target rocksdb_manifest_edit -- -j 4
```

## 选项

- --json: 设置程序将manifest转为json格式的文件(默认)
- --manifest: 设置程序将json格式的文件转换成manifest文件
- --jpath: json格式文件地址（必须是完整地址）
- --mpath: manifest格式文件地址（必须是完整地址）
- --help: 打印帮助信息

example:

```shell
./rocksdb_manifest_edit --json --jpath=/tmp/manifest.json --mpath=/usr/local/mysql/data/.rocksdb/MANIFEST-000315
```

