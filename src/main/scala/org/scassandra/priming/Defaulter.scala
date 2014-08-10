package org.scassandra.priming

import org.scassandra.cqlmessages.types.{CqlVarchar, ColumnType}

object Defaulter {
  def defaultColumnTypesToVarchar(columnTypes : Option[Map[String, ColumnType[_]]], resultsAsList: List[Map[String, Any]]) = {
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
      case Some(varTypes) => {
        val defaults = (0 until numberOfVariables).map(num => CqlVarchar).toList
        varTypes ++ (defaults drop varTypes.size)
      }
      case None => {
        (0 until numberOfVariables).map(num => CqlVarchar).toList
      }
    }
  }

}
