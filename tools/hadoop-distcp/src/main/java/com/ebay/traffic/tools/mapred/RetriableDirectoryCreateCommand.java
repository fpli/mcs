/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ebay.traffic.tools.mapred;

import org.apache.hadoop.conf.Configuration;
import com.ebay.traffic.tools.util.RetriableCommand;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

/**
 * This class extends Retriable command to implement the creation of directories
 * with retries on failure.
 */
public class RetriableDirectoryCreateCommand extends RetriableCommand {

  /**
   * Constructor, taking a description of the action.
   * @param description Verbose description of the copy operation.
   */
  public RetriableDirectoryCreateCommand(String description) {
    super(description);
  }

  /**
   * Implementation of RetriableCommand::doExecute().
   * This implements the actual mkdirs() functionality.
   * @param arguments Argument-list to the command.
   * @return Boolean. True, if the directory could be created successfully.
   * @throws Exception IOException, on failure to create the directory.
   */
  @Override
  protected Object doExecute(Object... arguments) throws Exception {
    assert arguments.length == 2 : "Unexpected argument list.";
    Path target = (Path)arguments[0];
    //Mapper.Context context = (Mapper.Context)arguments[1];

    //FileSystem targetFS = target.getFileSystem(context.getConfiguration());
    FileSystem targetFS = target.getFileSystem((Configuration)arguments[1]);
    return targetFS.mkdirs(target);
  }
}