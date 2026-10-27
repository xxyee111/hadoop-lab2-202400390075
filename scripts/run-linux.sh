#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
command -v hadoop >/dev/null
mkdir -p target/classes results
javac -encoding UTF-8 -cp "$(hadoop classpath)" -d target/classes src/main/java/edu/sdu/bigdata/InvertedIndex.java
jar cf target/hadoop-lab2.jar -C target/classes .
base="${LAB2_HDFS_BASE:-/user/$USER/lab2-$(date +%Y%m%d-%H%M%S)}"
hdfs dfs -mkdir -p "$base/input"
hdfs dfs -put data/documents/*.txt "$base/input/"
hdfs dfs -put data/stopwords.txt "$base/stopwords.txt"
hdfs dfs -ls "$base/input" | tee results/hdfs-input.txt
hadoop jar target/hadoop-lab2.jar edu.sdu.bigdata.InvertedIndex "$base/input" "$base/output" "$base/stopwords.txt" 1 2>&1 | tee results/job.log
hdfs dfs -test -e "$base/output/_SUCCESS"
hdfs dfs -cat "$base/output/part-r-*" > results/inverted-index.txt
python3 scripts/verify.py --output results/inverted-index.txt --summary results/verification.json | tee results/verification.txt
grep -E '^(address|checksum|datagram|ftp|http|protocol|tcp|udp)[[:space:]]' results/inverted-index.txt | tee results/sample-index.txt
yarn application -list -appStates FINISHED | tee results/yarn-applications.txt
echo "HDFS_BASE=$base"
