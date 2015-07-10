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
package kafka.security.auth;

/**
 * Different operations a client may perform on kafka resources.
 */

sealed trait Operation { def name: String}
case object Read extends Operation { val name: String = "Read" }
case object Write extends Operation { val name: String = "Write" }
case object Create extends Operation { val name: String = "Create" }
case object Delete extends Operation { val name: String = "Delete" }
case object Alter extends Operation { val name: String = "Alter" }
case object Describe extends Operation { val name: String = "Describe" }
case object ClusterAction extends Operation { val name: String = "ClusterAction" }
case object All extends Operation { val name: String = "All" }

object Operation {
   def fromString(operation: String) : Operation = {
      operation match {
         case operation if operation.equalsIgnoreCase(Read.name) => Read
         case operation if operation.equalsIgnoreCase(Write.name) => Write
         case operation if operation.equalsIgnoreCase(Create.name) => Create
         case operation if operation.equalsIgnoreCase(Delete.name) => Delete
         case operation if operation.equalsIgnoreCase(Alter.name) => Alter
         case operation if operation.equalsIgnoreCase(Describe.name) => Describe
         case operation if operation.equalsIgnoreCase(ClusterAction.name) => ClusterAction
         case operation if operation.equalsIgnoreCase(All.name) => All
      }
   }

   def values() : List[Operation] = {
      return List(Read, Write, Create, Delete, Alter, Describe, ClusterAction, All)
   }
}
