#!/usr/bin/env bash
set -euo pipefail
work="${LAB2_CLUSTER_DIR:-$HOME/lab2-cluster-20261027}"
mkdir -p "$work"
export HADOOP_HOME="${HADOOP_HOME:?Set HADOOP_HOME first}"
export HADOOP_CONF_DIR="$work/conf"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
export HADOOP_LOG_DIR="$work/logs"
export HADOOP_PID_DIR="$work/pids"
export HADOOP_HEAPSIZE_MAX=128
mkdir -p "$HADOOP_CONF_DIR" "$HADOOP_LOG_DIR" "$HADOOP_PID_DIR"
cp -R "$HADOOP_HOME/etc/hadoop/." "$HADOOP_CONF_DIR/"
export LAB2_WORK="$work"
python3 - <<'PY'
import os, pathlib, xml.etree.ElementTree as E
w=pathlib.Path(os.environ['LAB2_WORK'])
configs={
'core-site.xml':{'fs.defaultFS':'hdfs://localhost:19000','hadoop.tmp.dir':str(w/'tmp')},
'hdfs-site.xml':{'dfs.replication':'1','dfs.namenode.name.dir':str(w/'nn'),'dfs.datanode.data.dir':str(w/'dn'),'dfs.namenode.http-address':'localhost:19870','dfs.datanode.address':'localhost:19866','dfs.datanode.http.address':'localhost:19864','dfs.datanode.ipc.address':'localhost:19867'},
'mapred-site.xml':{'mapreduce.framework.name':'yarn','mapreduce.application.classpath':str(pathlib.Path(os.environ['HADOOP_HOME'])/'share/hadoop/mapreduce/*')+','+str(pathlib.Path(os.environ['HADOOP_HOME'])/'share/hadoop/mapreduce/lib/*'),'yarn.app.mapreduce.am.resource.mb':'384','yarn.app.mapreduce.am.command-opts':'-Xmx256m','mapreduce.map.memory.mb':'256','mapreduce.map.java.opts':'-Xmx160m','mapreduce.reduce.memory.mb':'256','mapreduce.reduce.java.opts':'-Xmx160m','mapreduce.jobhistory.address':'localhost:11020','mapreduce.jobhistory.webapp.address':'localhost:19888'},
'yarn-site.xml':{'yarn.resourcemanager.hostname':'localhost','yarn.resourcemanager.address':'localhost:18032','yarn.resourcemanager.scheduler.address':'localhost:18030','yarn.resourcemanager.resource-tracker.address':'localhost:18031','yarn.resourcemanager.admin.address':'localhost:18033','yarn.resourcemanager.webapp.address':'localhost:18088','yarn.nodemanager.aux-services':'mapreduce_shuffle','yarn.nodemanager.resource.memory-mb':'1024','yarn.scheduler.minimum-allocation-mb':'128','yarn.scheduler.maximum-allocation-mb':'1024','yarn.nodemanager.vmem-check-enabled':'false','yarn.nodemanager.pmem-check-enabled':'false','yarn.nodemanager.env-whitelist':'JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME','yarn.nodemanager.local-dirs':str(w/'nm-local'),'yarn.nodemanager.log-dirs':str(w/'nm-logs')}
}
for name,props in configs.items():
    root=E.Element('configuration')
    for k,v in props.items():
        p=E.SubElement(root,'property');E.SubElement(p,'name').text=k;E.SubElement(p,'value').text=v
    E.ElementTree(root).write(w/'conf'/name,encoding='utf-8',xml_declaration=True)
PY
export HADOOP_MAPRED_HOME="$HADOOP_HOME"
export HADOOP_COMMON_HOME="$HADOOP_HOME"
export HADOOP_HDFS_HOME="$HADOOP_HOME"
export HADOOP_YARN_HOME="$HADOOP_HOME"
test -f "$work/nn/current/VERSION" || hdfs namenode -format -nonInteractive
hdfs --daemon status namenode || hdfs --daemon start namenode
hdfs --daemon status datanode || hdfs --daemon start datanode
yarn --daemon status resourcemanager || yarn --daemon start resourcemanager
yarn --daemon status nodemanager || yarn --daemon start nodemanager
python3 - <<'PY'
import socket,time
for _ in range(300):
    try:
        with socket.create_connection(('localhost',19000),timeout=2):break
    except OSError:time.sleep(2)
else:raise RuntimeError('NameNode did not become ready')
PY
hdfs dfsadmin -safemode wait
python3 - <<'PY'
import urllib.request,json,time
for _ in range(300):
    try:
        nodes=json.load(urllib.request.urlopen('http://localhost:18088/ws/v1/cluster/nodes',timeout=3))
        if any(n['state']=='RUNNING' for n in nodes.get('nodes',{}).get('node',[])):break
    except Exception:pass
    time.sleep(2)
else:raise RuntimeError('YARN node did not become ready')
PY
jps
