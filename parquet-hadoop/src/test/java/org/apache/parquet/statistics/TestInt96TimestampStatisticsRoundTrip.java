/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.statistics;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.LocalDateTime;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestInt96TimestampStatisticsRoundTrip {
  private static final Logger LOG = LoggerFactory.getLogger(org.apache.parquet.statistics.TestInt96TimestampStatisticsRoundTrip.class);

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  private MessageType createSchema() {
    return Types.buildMessage()
        .required(INT96).named("timestamp_field")
        .named("root");
  }

  /**
   * Convert a timestamp string in format "yyyy-MM-dd HH:mm:ss.SSS" to INT96 bytes using NanoTime.
   * INT96 timestamps in Parquet are encoded as 12 bytes where:
   * - First 8 bytes: nanoseconds from midnight
   * - Last 4 bytes: Julian day
   */
  private Binary timestampToInt96(String timestamp) {
    LocalDateTime dt = LocalDateTime.parse(timestamp.replace(" ", "T"));
    long julianDay = dt.toLocalDate().toEpochDay() + 2440588; // Convert to Julian Day
    long nanos = dt.toLocalTime().toNanoOfDay();
    return new NanoTime((int)julianDay, nanos).toBinary();
  }

  private void writeParquetFile(Path file, Binary[] timestampValues) throws IOException {
    MessageType schema = createSchema();
    Configuration conf = new Configuration();
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
        .withConf(conf)
        .withType(schema)
        .build()) {
      for (Binary value : timestampValues) {
        Group group = new SimpleGroup(schema);
        group.add("timestamp_field", value);
        writer.write(group);
      }
    }
  }

  private void verifyStatistics(Path file, Binary minValue, Binary maxValue) throws IOException {
    Configuration conf = new Configuration();
    ParquetMetadata metadata = ParquetFileReader.readFooter(conf, file);
    
    // Verify INT96 statistics
    ColumnChunkMetaData timestampColumn = metadata.getBlocks().get(0).getColumns().get(0);
    Statistics<?> timestampStats = timestampColumn.getStatistics();
    
    assertTrue("INT96 statistics have non-null values", timestampStats.hasNonNullValue());
    assertEquals(Binary.fromConstantByteArray(timestampStats.getMinBytes()), minValue);
    assertEquals(Binary.fromConstantByteArray(timestampStats.getMaxBytes()), maxValue);
  }

  @Test
  public void testInt96TimestampStatistics() throws IOException {
    // Create test data with human-readable timestamps
    String[] timestamps = {
      "2020-01-01 00:00:00.000", // New Year 2020
      "2020-02-29 23:59:59.999", // Leap day 2020
      "2020-12-31 23:59:59.999", // End of 2020
      "2021-01-01 00:00:00.000", // Start of 2021
      "2023-06-15 12:30:45.500", // Mid-2023
      "2024-02-29 15:45:30.750", // Leap day 2024
      "2024-12-25 07:00:00.000", // Christmas 2024
      "2025-01-01 00:00:00.000", // New Year 2025
      "2025-07-04 20:00:00.000", // July 4th 2025
      "2025-12-31 23:59:59.999"  // End of 2025
    };

    Binary[] timestampValues = new Binary[timestamps.length];
    for (int i = 0; i < timestamps.length; i++) {
      timestampValues[i] = timestampToInt96(timestamps[i]);
    }

    Binary minValue = timestampToInt96("2020-01-01 00:00:00.000");
    Binary maxValue = timestampToInt96("2025-12-31 23:59:59.999");

    // Write and verify
    Path file = new Path(temp.getRoot().getPath(), "test_timestamps.parquet");
    writeParquetFile(file, timestampValues);
    verifyStatistics(file, minValue, maxValue);
  }
}
