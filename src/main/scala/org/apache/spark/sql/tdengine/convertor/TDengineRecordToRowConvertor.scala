package org.apache.spark.sql.tdengine.convertor

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object TDengineRecordToRowConvertor {

  def rawDataSchema = StructType(
    StructField("value", StringType) :: Nil)
}
