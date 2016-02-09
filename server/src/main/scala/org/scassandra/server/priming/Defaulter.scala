/*
 * Copyright (C) 2014 Christopher Batey and Dogan Narinc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra.server.priming

import org.scassandra.server.cqlmessages.types.{CqlVarchar, ColumnType}

object Defaulter {
  def defaultColumnTypesToVarchar(columnTypes: Option[Map[String, ColumnType[_]]], resultsAsList: List[Map[String, Any]]) = {
    val colTypes: Map[String, ColumnType[_]] =  columnTypes.getOrElse(Map[String, ColumnType[_]]())

    // check that all the columns in the rows have a type
    val columnNamesInAllRows: List[String] = resultsAsList.flatMap(row => row.keys).distinct
    val colNamesWithOnesNotInRows = (columnNamesInAllRows ++ colTypes.keys).distinct

    val colTypesWithDefaults : Map[String, ColumnType[_]] = colNamesWithOnesNotInRows.map(columnName => colTypes.get(columnName) match {
      case Some(columnType) => (columnName, columnType)
      case None => (columnName, CqlVarchar)
    }).toMap

    colTypesWithDefaults
  }

  def defaultVariableTypesToVarChar(numberOfVariables : Int, providedVariableTypes : Option[List[ColumnType[_]]]) : List[ColumnType[_]] = {
    providedVariableTypes match {
      case Some(varTypes) =>
        val defaults = (0 until numberOfVariables).map(num => CqlVarchar).toList
        varTypes ++ (defaults drop varTypes.size)
      case None =>
        (0 until numberOfVariables).map(num => CqlVarchar).toList
    }
  }

}
