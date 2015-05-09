import json
import os
import fnmatch


def find_files(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result


brokers    = ["c6501.ambari.apache.org",
              "c6502.ambari.apache.org",
              "c6503.ambari.apache.org",]
zookeepers = ["c6501.ambari.apache.org"]

producers =  ["c6501.ambari.apache.org",
              "c6502.ambari.apache.org"]

consumers =  ["c6501.ambari.apache.org"]

javaHome = "/usr/jdk64/jdk1.8.0_40"
kafkaHome = "/usr/hdp/current/kafka-broker"

zkPort = "2188"


def fix_cluster_config_files():
    for f in find_files("cluster_config.json", "/Users/rnaik/Projects/idea/kafka/system_test"):
        print " --> " + f
        inFile = open(f, "r")
        data = json.load(inFile)
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

        outFile = open(f, "w+")
        outFile.write( json.dumps(data, indent=4, separators=(',', ': ')) )
        outFile.close()


def fix_test_properties_files():
    for f in find_files("testcase_*_properties.json", "/Users/rnaik/Projects/idea/kafka/system_test"):
        inFile = open(f, "r")
        data = json.load(inFile)
        inFile.close()
        changed = False
        for entity in data["entities"]:
            if "zookeeper" in entity:
                entity["zookeeper"] = zookeepers[0] + ":" + zkPort
                changed = True

        if changed:
            print f
            outFile = open(f, "w+")
            outFile.write( json.dumps(data, indent=4, separators=(',', ': ')) )
            outFile.close()




# Main

# 1 Update all cluster_config.json files
fix_cluster_config_files()


# 2 Update testcase_*_properties.json files
fix_test_properties_files()


