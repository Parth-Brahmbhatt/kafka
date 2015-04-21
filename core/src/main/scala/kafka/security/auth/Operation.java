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
public enum Operation {
   READ,
   WRITE,
   CREATE,
   DELETE,
   EDIT,
   DESCRIBE,
   SEND_CONTROL_MSG,
   ALL;

   /**
    * method defined for case insensitive check. the default value of method is case sensitive
    */
   public static Operation fromString(String operationName) {
      if(operationName != null) {
         for(Operation operation: Operation.values()) {
            if(operationName.equalsIgnoreCase(operation.name())) {
               return operation;
            }
         }
      }
      throw new IllegalArgumentException("no matching enum value fonund for " + operationName);
   }
}
