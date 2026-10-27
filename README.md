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

当前保留的完整结果来自 Windows 上 Apache Hadoop 3.3.6 的真实 LocalJobRunner，使用 10 个 Mapper 和 1 个 Reducer。`results/local-job.log` 为原始运行记录，`results/local-verification.json` 为独立校验结果。另已实际验证关闭 Combiner，以及最大输入分片 65,536 字节加 2 个 Reducer 两种配置；两者与独立统计完全一致，见 `results/regression-tests.json`。

Linux HDFS/YARN 脚本已提供；上述本地验证不表示 Linux 伪分布式集群作业已经执行。公开仓库尚未创建，当前目录可直接作为待上传仓库。

## 参考

- Apache Hadoop MapReduce Tutorial: https://hadoop.apache.org/docs/r3.3.6/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
- Apache Hadoop Single Node Setup: https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-common/SingleCluster.html
- RFC Editor 原文地址见 `data/sources.json`。
