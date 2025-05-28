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

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.page.mem.MemPageStore;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.junit.Test;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class TestInt96TimestampStatisticsRoundTrip {

  private static final Random RANDOM = new Random(42);

  /**
   * Test INT96 statistics round trip.
   */
  @Test
  public void testInt96Stats() throws IOException {
    MessageType schema = Types.buildMessage()
        .required(PrimitiveTypeName.INT96).named("timestamp")
        .named("schema");

    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);

    Path path = new Path("target/test-int96-stats.parquet");
    OutputFile outputFile = HadoopOutputFile.fromPath(path, conf);

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
        .withConf(conf)
        .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY)
        .build()) {

      SimpleGroupFactory factory = new SimpleGroupFactory(schema);
      
      // Write some INT96 values
      for (int i = 0; i < 1000; i++) {
        Group record = factory.newGroup();
        Binary int96Value = generateRandomInt96();
        record.add("timestamp", int96Value);
        writer.write(record);
      }
    }

    // Read back and verify statistics
    InputFile inputFile = HadoopInputFile.fromPath(path, conf);
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      ParquetMetadata footer = reader.getFooter();
      List<BlockMetaData> rowGroups = footer.getBlocks();
      
      assertFalse("Should have at least one row group", rowGroups.isEmpty());
      
      for (BlockMetaData rowGroup : rowGroups) {
        List<ColumnChunkMetaData> columns = rowGroup.getColumns();
        assertEquals("Should have one column", 1, columns.size());
        
        ColumnChunkMetaData column = columns.get(0);
        Statistics<?> stats = column.getStatistics();
        
        assertNotNull("Statistics should not be null", stats);
        assertTrue("Statistics should have values", stats.hasNonNullValue());
        
        // Verify that min/max are properly ordered using INT96 comparison
        Binary min = Binary.fromConstantByteArray(stats.getMinBytes());
        Binary max = Binary.fromConstantByteArray(stats.getMaxBytes());
        
        int comparison = compareInt96(min, max);
        assertTrue("Min should be <= Max", comparison <= 0);
      }
    }
  }

  private Binary generateRandomInt96() {
    // Generate a random INT96 timestamp
    // INT96 has 12 bytes: 8 bytes nanoseconds + 4 bytes Julian day
    byte[] bytes = new byte[12];
    
    // Random nanoseconds (8 bytes)
    long nanos = Math.abs(RANDOM.nextLong());
    ByteBuffer buffer = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putLong(nanos);
    
    // Random Julian day (4 bytes) - use a reasonable range
    int julianDay = 2440000 + RANDOM.nextInt(30000); // ~1970 to ~2050
    buffer.putInt(julianDay);
    
    return Binary.fromConstantByteArray(buffer.array());
  }

  private int compareInt96(Binary b1, Binary b2) {
    ByteBuffer bb1 = b1.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
    ByteBuffer bb2 = b2.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
    
    // Compare Julian day first (last 4 bytes)
    int jd1 = bb1.getInt(8);
    int jd2 = bb2.getInt(8);
    if (jd1 != jd2) {
      return Integer.compareUnsigned(jd1, jd2) < 0 ? -1 : 1;
    }
    
    // If Julian days are equal, compare nanoseconds (first 8 bytes)
    long ns1 = bb1.getLong(0);
    long ns2 = bb2.getLong(0);
    if (ns1 != ns2) {
      return Long.compareUnsigned(ns1, ns2) < 0 ? -1 : 1;
    }
    
    return 0;
  }
} 