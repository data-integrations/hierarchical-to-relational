/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.spark;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.spark.sql.DataFrames;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

/**
 * Function to map from {@link Row} to {@link StructuredRecord}.
 */
public final class RowToRecord implements Function<Row, StructuredRecord> {

  private final Schema outputSchema;

  public RowToRecord(Schema outputSchema) {
    this.outputSchema = outputSchema;
  }

  @Override
  public StructuredRecord call(Row row) throws Exception {

    Schema rowSchema = DataFrames.toSchema(row.schema());
    StructuredRecord structuredRecord = DataFrames.fromRow(row, rowSchema);

    if (outputSchema == null || outputSchema.getFields() == null) {
      throw new Exception("Invalid output schema");
    }

    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (Schema.Field field : outputSchema.getFields()) {
      builder.set(field.getName(), structuredRecord.get(field.getName()));
    }
    return builder.build();
  }
}
