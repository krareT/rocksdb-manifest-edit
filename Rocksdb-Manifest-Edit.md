# Rocksdb-Manifest-Edit

## 从 RocksDB 说起

RocksDB 是 Facebook 基于 Google 开源的 LevelDB，并在此基础上加以改进、开源的一个嵌入式 key-value 数据库存储引擎，其键和值是任意的字节流，它支持高效的查找和范围查询，支持高负载的随机读、高负载的更新操作或两者的结合。

RocksDB 是基于 LSM tree 存储的，其包含三个基本结构：MemTable，SST file，log file。其中 MemTable 是一个内存数据结构，每当有新的数据插入时，会被插入到 MemTable 并且追加到 logfile 中，当 MemTable 被写满的时候，其中的数据会被刷新到 SST file 中。而 SST file 中的数据经过排序，可以加快键的查找。

每当有一个 `Get()` 请求的时候，RocksDB 会检查可修改的 MemTable，不变的 MemTable 和 SST file 以查找 key，其中 SST file 是通过 level 来组织的。在 level 0，SST file 是基于被刷新到文件的时间排序的，它们的键的范围（被定义为 `FileMetaData.smallest` 和 `FileMetaData.largest`）会相互重叠，所以需要查找每一个在  level 0 的 SST file。但是读操作会随着 SST file 个数增加而变慢，RocksDB 通过周期性的合并文件，来保持 SST file 的个数。

不同于 LevelDB 的单线程合并，RocksDB 支持多线程合并，而LSM 型的数据结构，最大的性能问题就出现在其合并的时间损耗上。RocksDB 在多 CPU 的环境下，多线程合并速度是 LevelDB 所无法比拟的，其速度可以比 LevelDB 快十倍或更多。每次在添加新文件和删除文件之后合并的时候，都会将这些操作记录同步到 MANIFEST 文件中，所以 MANIFEST 文件中记录了数据库的状态。

## MANIFEST 文件

因为文件系统不是原子性的，而这在系统出错的情况下容易导致不一致的情况出现，即使是开启了日志，依旧不能保证 RocksDB 的一致性，且 POSIX 文件系统不支持原子的批量操作，所以其使用 MANIFEST 文件来记录 RocksDB 状态的变化。

在系统启动或者重启时，最新的 MANIFEST 日志文件包含与 RocksDB 一致的状态，任何一个后来的状态改变都会被写入到 MANIFEST 日志文件中。当一个 MANIFEST 文件超过了配置的最大值的时候，一个包含当前 RocksDB 状态信息的新的 MANIFEST 文件就会创建，CURRENT 文件会记录最新的 MANIFEST 文件信息。当所有的更改都同步到文件系统之后，之前老的 MANIFEST 文件就会被清除。

在任意时间，RocksDB 的一个确定的状态可以被看成是一个 Version (或者说快照），每一个对 Version 的修改都看做是 VersionEdit，一个 Version 是由 VersionEdit 序列所构造而成。所以实际上，一个 MANIFEST 就是由 VersionEdit 序列构成的。

## 为什么要解析 MANIFEST 文件

RocksDB 的一致性完全依赖于 MANIFEST 文件，一旦 MANIFEST 文件出错或丢失，那么整个数据库便只能作废。在 RocksDB 运行正常自然是一切都好，但是一旦出现错误，MANIFEST 文件出错，那么在RocksDB自身没有提供修复 MANIFEST 文件的情况下，我们只能自行去进行修复。

RocksDB 本身实际上提供了一个修复 MANIFEST 的方式，就是使用 ldb 的子命令 repair 来修复 MANIFEST 文件，但是这个方法，只能解决 MANIFEST 损坏或者丢失的情况，且不能保证解决所有情况。如果出现 SST file 丢失或损坏的情况，即使 MANIFEST 没有任何损坏，依旧会导致 RocksDB 无法运行。所以我们需要有能够修复修改 MANIFEST 文件的能力。

因为要有修复修改 MANIFEST 文件的能力，所以必须要先知道 MANIFEST 文件内部存储的信息是什么，是怎样存储的，这样就必须要解析出 MANIFEST 文件。在知道 MANIFEST 文件存储的信息是什么之后，我们才能通过修改这些信息并将这些信息写回 MANIFEST文件的方式，获得修复修改 MANIFEST 文件的能力。

## 如何解析 MANIFEST 文件

首先解析 MANIFEST 文件我们需要明确一点，MANIFEST 文件是以什么样的形式存储 VersionEdit 序列的。在 Linux 中将 MANIFEST 文件 cat 出来看到的是一堆似乎能看懂的东西。事实上如果 MANIFEST 文件能够简单明了的表现出 VersionEdit 序列就不必我们进行解析了。

查阅 RocksDB 代码，知道实际上 MANIFEST 文件存储的是每个 VersionEdit 经过二进制编码之后的形式，所以如果想要解析 MANIFEST 文件，那么势必需要知道 VersionEdit 是怎么样编码的。不过庆幸的是，VersionEdit 提供了 `Status DecodeFrom(const Slice& src)` 这个函数，能够直接将 MANIFEST 文件的每一条记录解析到 VersionEdit。

在得到 VersionEdit 之后，我们仍然需要选择一个合适的格式来将 VersionEdit 变成我们可直接阅读的样子，在这里，我们选择了 JSON。因为 JSON 是一个轻量级的数据交换格式，易于人阅读和编写，也易于机器进行解析和生成。

生成 JSON 的时候，我们选择采用 [nlohmann 开源的 json 库](https://github.com/nlohmann/json)，使用这个库可以非常简单的利用键值对便可以生成 JSON。但是使用这个库，因为其默认使用的是 `std::map`，其默认按照键进行排序，不会保留原本设定的添加顺序，这样使得 VersionEdit 阅读起来上下文相关性极差。所以我们基于这个库，自定义了一种类型 [`JsonStrMap`](https://github.com/Terark/terichdb/blob/master/src/terark/terichdb/json.hpp)，这个类型能够按照添加进 json 的顺序将键值对输出，使得最后解析完成输出的 JSON 能够按照 VersionEdit 自身成员的相关性进行排序输出。这样方便了我们对每一个 VersionEdit 进行阅读。

因为 JSON 的优点所以我们选择了 JSON，如果对其它格式更加熟悉的，也可以利用工具将 JSON 转换为其它熟悉的格式，这里不提供其它可选的格式。

## 如何转换到 MANIFEST 文件

我们解析 MANIFEST 文件的目的是为了能够对 MANIFEST 文件进行修改修复使得整个数据库能够更健壮，即使丢了部分数据或者损失了部分数据也能够取出剩下的数据。所以当我们解析完成 MANIFEST 文件之后，我们需要把解析完成的 JSON 转换回 MANIFEST 文件。

我们知道 MANIFEST 实际上存储的是 VersionEdit 序列的二进制形式，所以我们首先需要从 JSON 解析回 VersionEdit，之后再将其转换为二进制形式。将 JSON 解析成 VersionEdit 并不难，nlohmann 开源的 json 库本身能够直接从 json 类型的文件读取出 JSON，所以我们只需要遍历 JSON，依照键将其存入 VersionEdit 便可以得到我们想要的 VersionEdit 了。

但是实际操作上，依旧不能直接将值直接存入 VersionEdit 中，部分值仍然需要经过处理才可以存入 VersionEdit 中，比如 VersionEdit 中的 InternalKey，在写入到 JSON 的时候，是将其中的二进制转换为十六进制字符串存储在 JSON 中的，所以存入到 InternalKey 的时候，同样需要将其转换为二进制。

在得到所需的 VersionEdit 的时候，我们并不需要自己再一点一点的将 VersionEdit 编码成二进制形式，只需要调用 RocksDB 自身提供的函数 `bool EncodeTo(std::string* dst)` 即可。当所有的 VersionEdit 都被编码完成之后，写入到 MANIFEST 文件即可。

写入到 MANIFEST 文件并不是一件简单的事情，如果直接将 VersionEdit 编码完成的二进制字符串写入到 MANIFEST 文件中实际上会造成 MANIFEST 文件信息的部分丢失，所以采用 RocksDB 中自有的方法。利用 VersionSet 中的 `log_descriptor` 的 `AddRecord()` 方法先将每个 VersionEdit 编码后的二进制字符串记录，等所有的 VersionEdit 编码完成后，使用 `SyncManifest()` 函数便可以将所有的记录写入到 MANIFEST 文件中。

## 其它功能

在实际使用这个工具的时候，我们可能不止是需要解析一个 MANIFEST 文件，一个个去解析的话，因为需要给定参数，未免会有些麻烦。所以我们提供了一个批量解析的方式。这个方式首先会检查给定的目录下的所有文件，然后顺次执行，一旦出错便会停止，全部成功后便可得到所有与原文件名称类似的文件。

## 总结

尽管 RocksDB 是一个非常优秀、性能非常突出的数据库存储引擎，但是因为文件系统不是原子性的，POSIX 系统也不支持原子的批量操作，所以 RocksDB 并不会将自己的一些元数据存放到自己的 key-value 系统里面，而是使用了单独的一个 MANIFEST 文件。而当 MANIFEST 文件出错，或者是 SST file 出错的情况下，如果我们没有能够修改修复 MANIFEST 文件的能力，那么就会使得 RocksDB 中存储的所有数据都会作废。这是我们所不能接受的。

所以编写这个项目，使得我们具备了修改修复 MANIFEST 文件的能力，能够在 RocksDB 出错的情况下，最大的减少我们的损失。