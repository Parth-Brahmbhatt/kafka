/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.admin

import java.util.Properties

import joptsimple._
import kafka.security.auth._
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.utils.Utils

object AclCommand {

  val delimter: String = ",";
  val nl = System.getProperty("line.separator")

  def main(args: Array[String]): Unit = {

    val opts = new AclCommandOptions(args)
    
    if(args.length == 0) {
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --list, --describe, --create, --alter or --delete")
    }

    val authZ: Authorizer = CoreUtils.createObject(opts.options.valueOf(opts.authorizerClassNameOpt))
    val props: Properties = new Properties()
    props.put(KafkaConfig.ZkConnectProp, opts.options.valueOf(opts.zkConnectOpt))
    val kafkaConfig: KafkaConfig = KafkaConfig.fromProps(props)
    authZ.initialize(kafkaConfig)

    val actions = Seq(opts.addOpt, opts.removeOpt).count(opts.options.has _)
    if(actions != 1)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --list, --add, --remove.")

    opts.checkArgs()

    try {
      if(opts.options.has(opts.addOpt))
        addAcl(authZ, opts)
      else if(opts.options.has(opts.removeOpt))
        removeAcl(authZ, opts)
      else if(opts.options.has(opts.listOpt))
        listAcl(authZ, opts)
    } catch {
      case e: Throwable =>
        println("Error while executing topic command " + e.getMessage)
        println(Utils.stackTrace(e))
    }
  }

  private def addAcl(authZ: Authorizer, opts: AclCommandOptions): Unit = {
    val acls: Set[Acl] = getAcl(opts)
    println("Adding acls: " + nl + acls.map("\t" + _).mkString(nl) + nl)
    authZ.addAcls(acls, getResource(opts))
    listAcl(authZ, opts)
  }

  private def removeAcl(authZ: Authorizer, opts: AclCommandOptions): Unit = {
    val acls: Set[Acl] = getAcl(opts)
    if(acls.isEmpty) {
      if(confirmaAction("Are you sure you want to delete all acls for resource: " + getResource(opts) + " y/n?"))
        authZ.removeAcls(getResource(opts))
    } else {
      if(confirmaAction(("Are you sure you want to remove acls: " + nl + acls.map("\t" + _).mkString(nl) + nl) + "  from resource " + getResource(opts)+ " y/n?"))
        authZ.removeAcls(acls, getResource(opts))
    }

    listAcl(authZ, opts)
  }

  private def listAcl(authZ: Authorizer, opts: AclCommandOptions): Unit = {
    val acls: Set[Acl] = authZ.getAcls(getResource(opts))
    println("Following is list of  acls for resource : " + getResource(opts) + nl + acls.map("\t" + _).mkString(nl) + nl)
  }

  private def getAcl(opts: AclCommandOptions) : Set[Acl] = {
    val allowedPrincipals: Set[KafkaPrincipal] = if(opts.options.has(opts.allowPrincipalsOpt))
      opts.options.valueOf(opts.allowPrincipalsOpt).toString.split(delimter).map(s => KafkaPrincipal.fromString(s)).toSet
    else
      if(opts.options.has(opts.allowHostsOpt)) Set[KafkaPrincipal](Acl.wildCardPrincipal) else Set.empty[KafkaPrincipal]

    val deniedPrincipals: Set[KafkaPrincipal] = if(opts.options.has(opts.denyPrincipalsOpt))
      opts.options.valueOf(opts.denyPrincipalsOpt).toString.split(delimter).map(s => KafkaPrincipal.fromString(s)).toSet
    else
      if(opts.options.has(opts.denyHostssOpt)) Set[KafkaPrincipal](Acl.wildCardPrincipal) else Set.empty[KafkaPrincipal]

    val allowedHosts: Set[String] = if(opts.options.has(opts.allowHostsOpt))
      opts.options.valueOf(opts.allowHostsOpt).toString.split(delimter).toSet
    else
      if(opts.options.has(opts.allowPrincipalsOpt)) Set[String](Acl.wildCardHost) else Set.empty[String]

    val deniedHosts = if(opts.options.has(opts.denyHostssOpt))
      opts.options.valueOf(opts.denyHostssOpt).toString.split(delimter).toSet
    else
      if(opts.options.has(opts.denyPrincipalsOpt)) Set[String](Acl.wildCardHost) else Set.empty[String]

    val allowedOperations: Set[Operation] = if(opts.options.has(opts.operationsOpt))
      opts.options.valueOf(opts.operationsOpt).toString.split(delimter).map(s => Operation.fromString(s)).toSet
    else
      Set[Operation](Operation.ALL)

    var acls: collection.mutable.HashSet[Acl] = new collection.mutable.HashSet[Acl]
    if(allowedHosts.nonEmpty || allowedPrincipals.nonEmpty )
      acls += new Acl(allowedPrincipals, PermissionType.ALLOW, allowedHosts, allowedOperations)

    if(deniedHosts.nonEmpty || deniedPrincipals.nonEmpty )
      acls += new Acl(deniedPrincipals, PermissionType.DENY, deniedHosts, allowedOperations)

    acls.toSet
  }

  private def getResource(opts: AclCommandOptions) : String = {
    if(opts.options.has(opts.topicOpt))
      return opts.options.valueOf(opts.topicOpt).toString
    else if(opts.options.has(opts.clusterOpt))
      return "kafka-cluster"
    else if(opts.options.has(opts.groupOpt))
      return opts.options.valueOf(opts.groupOpt).toString
    else
      println("You must provide at least one of the resource argument from --topic <topic>, --cluster or --consumer-group <group>")
      System.exit(1)

    null
  }

  private def confirmaAction(msg: String): Boolean = {
    println(msg)
    val userInput: String = Console.readLine()

    "y".equalsIgnoreCase(userInput)
  }

  class AclCommandOptions(args: Array[String]) {
    val parser = new OptionParser
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
                                      "Multiple URLS can be given to allow fail-over. This zookeeper may get used as the acl store by authorizer.")
                           .withRequiredArg
                           .describedAs("urls")
                           .ofType(classOf[String])
    val authorizerClassNameOpt = parser.accepts("authorizer", "REQUIRED: fully qualified class name of the authorizer implementation i.e. kafka.security.auth.SimpleAclAuthorizer")
      .withRequiredArg
      .describedAs("authorizer")
      .ofType(classOf[String])

    val topicOpt = parser.accepts("topic", "topic to which acls should be added or removed.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
    val clusterOpt = parser.accepts("cluster", "add/remove cluster action acls.")
    val groupOpt = parser.accepts("consumer-group", "add remove acls for a group")
      .withRequiredArg
      .describedAs("consumer-group")
      .ofType(classOf[String])

    val addOpt = parser.accepts("add", "Add indicates you are trying to add acls.")
    val removeOpt = parser.accepts("remove", "Indicates you are trying to remove an acl.")
    val listOpt =  parser.accepts("list", "list acls for the specifed resource, use --topic <topic> or --consumer-group <group> or --cluster")

    val operationsOpt = parser.accepts("operations", "comma separated list of operations, allowed Operations are: " + nl +
      Operation.values().map("\t" + _).mkString(nl) + nl)
      .withRequiredArg
      .ofType(classOf[String])

    val allowPrincipalsOpt = parser.accepts("allowprincipals", "comma separated list of principals where principal is in principalType: name format")
      .withRequiredArg
      .describedAs("allowprincipals")
      .ofType(classOf[String])

    val denyPrincipalsOpt = parser.accepts("denyprincipasl", "comma separated list of principals where principal is in principalType: name format")
      .withRequiredArg
      .describedAs("denyPrincipalsOpt")
      .ofType(classOf[String])

    val allowHostsOpt = parser.accepts("allowhosts", "comma separated list of principals where principal is in principalType: name format")
      .withRequiredArg
      .describedAs("allowhosts")
      .ofType(classOf[String])

    val denyHostssOpt = parser.accepts("denyhosts", "comma separated list of principals where principal is in principalType: name format")
      .withRequiredArg
      .describedAs("denyhosts")
      .ofType(classOf[String])

    val helpOpt = parser.accepts("help", "Print usage information.")

    val options = parser.parse(args : _*)

    def checkArgs() {
      // check required args
      CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt, authorizerClassNameOpt)
    }
  }
  
}
