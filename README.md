# 实验二 文档倒排索引算法实现

学号：202400390075；姓名：李紫嫣。

## 数据与结果

`data/documents/` 保存从 RFC Editor 公开网页下载的 10 篇英文技术文档，RFC 编号为 768、791、792、793、826、959、1034、1035、2616、9110。`data/sources.json` 记录原始地址、标题、字节数、下载日期和 SHA-256，原文中的作者、版权和许可说明保持原样。它们是实验语料，不代表当前协议选型建议。

`data/stopwords.txt` 为按字典序排列的 128 个英文停词。分词统一转为小写，取正则表达式 `[a-z]+` 匹配的连续英文字母，数字及标点作为分隔符。保留正文、页眉、页脚、参考文献及附录；不做词干化，不按行拼接连字符单词。

`results/inverted-index.txt` 是完整 Hadoop 输出，格式为 `word<TAB><document,count>;...;<total,count>.`，共 6,267 个词条和 16,091 个文档词频项。原始词数 210,374，停词过滤 81,555，保留词数 128,819。文件名按字典序排列；默认 1 个 Reducer，单词全局升序。

## 算法

Mapper 获取 FileSplit 文件名，按行分词、过滤停词，在行内合并重复词，输出 `word -> document<TAB>count`。Combiner 按文档累加词频，保持相同中间结构。Reducer 按单词分组，使用 TreeMap 汇总各文档词频，并输出总词频。计数使用 long。无论 Combiner 被执行零次、一次还是多次，结果都相同；文档拆成多个输入分片时也会按文件名正确归并。停词通过分布式缓存加载；驱动程序拒绝重复文件名、非法文件名以及已存在的输出目录。

## Linux 运行

要求 Linux、JDK 8、Python 3 和 Apache Hadoop 3.3.6。已经有 HDFS/YARN 集群时：

```bash
export JAVA_HOME=/path/to/jdk8
export HADOOP_HOME=/path/to/hadoop-3.3.6
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
bash scripts/run-linux.sh
```

没有集群时，先在当前 shell 中启动独立的单节点伪分布式环境，再运行：

```bash
source scripts/start-single-node.sh
bash scripts/run-linux.sh
```

脚本使用独立目录 `$HOME/lab2-cluster-20261027`，HDFS RPC 端口 19000，NameNode 页面 19870，ResourceManager 页面 18088，避免覆盖既有 Hadoop 数据。只在 NameNode 元数据尚不存在时格式化。启动前应确认端口未被其他服务占用；同一集群已启动时不必重复启动。

也可以运行 `mvn package` 编译，Hadoop 依赖为 provided，运行时由 Hadoop 提供。

## 独立校验

```bash
python3 scripts/verify.py --output results/inverted-index.txt --summary results/verification.json
```

校验脚本独立读取原始文档并用 Python Counter 重算每一个单词和每一篇文档的频次，同时检查语料 SHA-256、停词排除、输出格式、文档顺序、词频总和以及重复词条。失败时以非零状态退出。

## 已执行的验证范围

主要结果来自 Ubuntu 22.04.5 LTS、Zulu OpenJDK 1.8.0_512 和 Apache Hadoop 3.3.6 的真实 HDFS/YARN 单节点伪分布式集群。虚拟机配置为 1 个 CPU、2 GB 内存。已实际向 HDFS 上传全部10篇文档及停词表，提交10个Mapper、1个Reducer的任务。

- YARN应用编号：`application_1793109981654_0001`。
- MapReduce作业编号：`job_1793109981654_0001`。
- 最终状态：`FINISHED / SUCCEEDED`；HDFS输出存在 `_SUCCESS`。
- Linux结果：`results/inverted-index.txt`；原始日志：`results/job.log`。
- 独立完整校验：`results/verification.json`；全部6,267个词条、16,091项文档词频及total均一致。
- 实际集群配置：`results/configuration/`。原始终端截图将在桌面解锁后加入。
- HDFS输入清单和YARN原始状态也保存在 `results/`。

补充验证在 Windows Hadoop 3.3.6 LocalJobRunner 上实际执行：默认配置、关闭Combiner、最大分片65,536字节加2个Reducer。所有配置均通过独立全文重算，且Linux完整索引与Windows默认完整索引逐字节一致。记录见 `results/local-job.log`、`results/local-verification.json` 和 `results/regression-tests.json`。

公开仓库：https://github.com/xxyee111/hadoop-lab2-202400390075

## 参考

- Apache Hadoop MapReduce Tutorial: https://hadoop.apache.org/docs/r3.3.6/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
- Apache Hadoop Single Node Setup: https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-common/SingleCluster.html
- RFC Editor 原文地址见 `data/sources.json`。
