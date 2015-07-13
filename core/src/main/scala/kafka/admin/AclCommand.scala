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

import java.io.{File, FileInputStream}
import java.util.Properties

import joptsimple._
import kafka.security.auth._
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.utils.Utils

object AclCommand {

  val delimeter: String = ",";
  val nl = System.getProperty("line.separator")

  def main(args: Array[String]): Unit = {

    val opts = new AclCommandOptions(args)

    opts.checkArgs()

    val actions = Seq(opts.addOpt, opts.removeOpt, opts.listOpt).count(opts.options.has _)
    if(actions != 1)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --list, --add, --remove.")

    val props: Properties = new Properties()
    props.load(new FileInputStream(new File(opts.options.valueOf(opts.config))))
    val kafkaConfig: KafkaConfig = KafkaConfig.fromProps(props)
    val authZ: Authorizer = CoreUtils.createObject(kafkaConfig.authorizerClassName)
    authZ.initialize(kafkaConfig)

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
      opts.options.valueOf(opts.allowPrincipalsOpt).toString.split(delimeter).map(s => KafkaPrincipal.fromString(s)).toSet
    else if(opts.options.has(opts.allowHostsOpt))
      Set[KafkaPrincipal](Acl.WildCardPrincipal)
    else
      Set.empty[KafkaPrincipal]

    val deniedPrincipals: Set[KafkaPrincipal] = if(opts.options.has(opts.denyPrincipalsOpt))
      opts.options.valueOf(opts.denyPrincipalsOpt).toString.split(delimeter).map(s => KafkaPrincipal.fromString(s)).toSet
    else
      Set.empty[KafkaPrincipal]

    val allowedHosts: Set[String] = if(opts.options.has(opts.allowHostsOpt))
      opts.options.valueOf(opts.allowHostsOpt).toString.split(delimeter).toSet
    else if(opts.options.has(opts.allowPrincipalsOpt))
      Set[String](Acl.WildCardHost)
    else
      Set.empty[String]

    val deniedHosts = if(opts.options.has(opts.denyHostssOpt))
      opts.options.valueOf(opts.denyHostssOpt).toString.split(delimeter).toSet
    else if(opts.options.has(opts.denyPrincipalsOpt))
      Set[String](Acl.WildCardHost)
    else
      Set.empty[String]

    val allowedOperations: Set[Operation] = if(opts.options.has(opts.operationsOpt))
      opts.options.valueOf(opts.operationsOpt).toString.split(delimeter).map(s => Operation.fromString(s)).toSet
    else
      Set[Operation](All)

    var acls: collection.mutable.HashSet[Acl] = new collection.mutable.HashSet[Acl]
    if(allowedHosts.nonEmpty && allowedPrincipals.nonEmpty )
      acls += new Acl(allowedPrincipals, Allow, allowedHosts, allowedOperations)

    if(deniedHosts.nonEmpty && deniedPrincipals.nonEmpty )
      acls += new Acl(deniedPrincipals, Deny, deniedHosts, allowedOperations)

    if(acls.isEmpty)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must specify one of --allowprincipals, --denyprincipals, --allowhosts, --denyhosts.")

    acls.toSet
  }

  private def getResource(opts: AclCommandOptions) : Resource = {
    if(opts.options.has(opts.topicOpt))
      return new Resource(Topic, opts.options.valueOf(opts.topicOpt).toString)
    else if(opts.options.has(opts.clusterOpt))
      return Resource.ClusterResource
    else if(opts.options.has(opts.groupOpt))
      return new Resource(ConsumerGroup, opts.options.valueOf(opts.groupOpt).toString)
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
    val config = parser.accepts("config", "REQUIRED: Path to a server.properties file")
      .withRequiredArg
      .describedAs("config")
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
    val listOpt =  parser.accepts("list", "list acls for the specified resource, use --topic <topic> or --consumer-group <group> or --cluster")

    val operationsOpt = parser.accepts("operations", "comma separated list of operations, default is All. Valid operation names are: " + nl +
      Operation.values().map("\t" + _).mkString(nl) + nl)
      .withRequiredArg
      .ofType(classOf[String])

    val allowPrincipalsOpt = parser.accepts("allowprincipals", "comma separated list of principals where principal is in principalType:name format. " +
      "If you have specified --allowhosts then the default for this option will be set to user:* which allows access to all users.")
      .withRequiredArg
      .describedAs("allowprincipals")
      .ofType(classOf[String])

    val denyPrincipalsOpt = parser.accepts("denyprincipals", "comma separated list of principals where principal is in principalType: name format. Be default anyone not in --allowprincipals list is denied access. " +
      "You only need to use this option when you have given access to some super set of users and want to disallow access to some of the users in that set. " +
      "For example if you wanted to allow access to all users in the system but test user you can define an acl that allows access to user:* and specify --denyprincipals=user:test@EXAMPLE.COM. " +
      "AND PLEASE REMEMBER DENY RULES TAKES PRECEDENCE OVER ALLOW RULES.")
      .withRequiredArg
      .describedAs("denyPrincipalsOpt")
      .ofType(classOf[String])

    val allowHostsOpt = parser.accepts("allowhosts", "comma separated list of hosts from which principals listed in --allowprincipals will have access." +
      "If you have specified --allowprincipals then the default for this option will be set to * which allows access from all hosts.")
      .withRequiredArg
      .describedAs("allowhosts")
      .ofType(classOf[String])

    val denyHostssOpt = parser.accepts("denyhosts", "comma separated list of hosts from which principals listed in --denyprincipals will be denied access. " +
      "If you have specified --denyprincipals then the default for this option will be set to * which allows access from all hosts.")
      .withRequiredArg
      .describedAs("denyhosts")
      .ofType(classOf[String])

    val helpOpt = parser.accepts("help", "Print usage information.")

    val options = parser.parse(args : _*)

    def checkArgs() {
      // check required args
      CommandLineUtils.checkRequiredArgs(parser, options, config)
    }
  }
}