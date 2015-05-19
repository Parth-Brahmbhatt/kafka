#!/usr/bin/python
import sys
import json
import os
import fnmatch
import collections
import fileinput
import re

def find_files(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result


brokers    = ["c6501.ambari.apache.org",
              "c6502.ambari.apache.org",
              "c6503.ambari.apache.org"]

zookeepers = ["c6501.ambari.apache.org"]

producers =  ["c6501.ambari.apache.org",
              "c6502.ambari.apache.org"]

consumers =  ["c6501.ambari.apache.org"]

javaHome = "/usr/jdk64/jdk1.8.0_40"
kafkaHome = "/usr/hdp/current/kafka-broker"

zkPort = "2181"



def fix_cluster_config_files(directory):
    for f in find_files("cluster_config.json", directory):
        print "Processing " + f
        inFile = open(f, "r")
        data = json.load(inFile, object_pairs_hook=collections.OrderedDict)
        inFile.close()

        brokerIndx = 0
        producerIndx = 0
        consumerIndx = 0
        zkIndx = 0

        for entity in data["cluster_config"]:
            if entity["role"] == "broker":
                entity["hostname"] = brokers[brokerIndx]
                brokerIndx = brokerIndx+1 if brokerIndx+1<len(brokers) else 0
            elif entity["role"] == "zookeeper":
                entity["hostname"] = zookeepers[zkIndx]
                zkIndx = zkIndx+1 if zkIndx+1<len(zookeepers) else 0
            elif entity["role"] == "producer_performance":
                entity["hostname"] = producers[producerIndx]
                producerIndx = producerIndx+1 if producerIndx+1<len(producers) else 0
            elif entity["role"] == "console_consumer":
                entity["hostname"] = consumers[consumerIndx]
                consumerIndx = consumerIndx+1 if consumerIndx+1<len(consumers) else 0

            if "java_home" in entity:
                entity["java_home"] = javaHome
            if "kafka_home" in entity:
                entity["kafka_home"] = kafkaHome

        outFile = open(f, "w+")
        outFile.write( json.dumps(data, indent=4, separators=(',', ': ')) )
        outFile.close()


def fix_json_properties_files(directory):
    for f in find_files("testcase_*_properties.json", directory):
        print "Processing " + f
        inFile = open(f, "r")
        data = json.load(inFile, object_pairs_hook=collections.OrderedDict)
        inFile.close()
        changed = False
        for entity in data["entities"]:
            if "zookeeper" in entity:
                entity["zookeeper"] = zookeepers[0] + ":" + zkPort
                changed = True

        if changed:
            outFile = open(f, "w+")
            outFile.write( json.dumps(data, indent=4, separators=(',', ': ')) )
            outFile.close()

def fix_other_properties_file(directory):
    for f in find_files("*.properties", directory):
        print "Processing " + f
        print os.popen("perl -i -pe 's/zookeeper.connect=localhost:.*/zookeeper.connect=" + zookeepers[0] + ":" + zkPort + "/' " + f).read()
        print os.popen("perl -i -pe 's/zk.connect=localhost:.*/zk.connect=" + zookeepers[0] + ":" + zkPort + "/' " + f).read()

        if re.search("zookeeper_.*properties", f):
            print os.popen("perl -i -pe 's/server.1=localhost/server.1=" + zookeepers[0] + "/' " + f).read()


# Main
#Usage  genclusterconf.py systest_dir
directory = sys.argv[1]  # "/Users/rnaik/Projects/idea/kafka/system_test"

# 1 Update all cluster_config.json files
fix_cluster_config_files(directory)


# 2 Update testcase_*_properties.json files
fix_json_properties_files(directory)

# 3 fix role specific property files
fix_other_properties_file(directory)
