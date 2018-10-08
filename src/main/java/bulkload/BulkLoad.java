/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bulkload;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.lang.System;

import org.json.JSONObject;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

/**
 * Usage: java bulkload.BulkLoad
 */
public class BulkLoad
{
    /** Default output directory */
    public static final String DEFAULT_OUTPUT_DIR = "./data";

    /** Keyspace name */
    public static final String KEYSPACE = "mappings";

    /**
     * Schema for bulk loading table.
     * It is important not to forget adding keyspace name before table name,
     * otherwise CQLSSTableWriter throws exception.
     */
    public static final String PROFILES_SCHEMA = "CREATE TABLE mappings.profiles (\n" +
            "    ssp text,\n" +
            "    id text,\n" +
            "    marker text,\n" +
            "    PRIMARY KEY ((ssp, id))\n" +
            ") WITH bloom_filter_fp_chance = 0.01\n" +
            "    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}\n" +
            "    AND comment = ''\n" +
            "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}\n" +
            "    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n" +
            "    AND crc_check_chance = 1.0\n" +
            "    AND dclocal_read_repair_chance = 0.1\n" +
            "    AND default_time_to_live = 2592000\n" +
            "    AND gc_grace_seconds = 864000\n" +
            "    AND max_index_interval = 2048\n" +
            "    AND memtable_flush_period_in_ms = 0\n" +
            "    AND min_index_interval = 128\n" +
            "    AND read_repair_chance = 0.0\n" +
            "    AND speculative_retry = '99PERCENTILE';";

    /**
     * Schema for bulk loading table.
     * It is important not to forget adding keyspace name before table name,
     * otherwise CQLSSTableWriter throws exception.
     */
    public static final String SEGMENTS_SCHEMA = "CREATE TABLE mappings.segments_mapping (\n" +
            "    marker text,\n" +
            "    segment text,\n" +
            "    \"sourceId\" text,\n" +
            "    type text,\n" +
            "    PRIMARY KEY (marker, segment, \"sourceId\", type)\n" +
            ") WITH CLUSTERING ORDER BY (segment ASC, \"sourceId\" ASC, type ASC)\n" +
            "    AND bloom_filter_fp_chance = 0.01\n" +
            "    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}\n" +
            "    AND comment = ''\n" +
            "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}\n" +
            "    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n" +
            "    AND crc_check_chance = 1.0\n" +
            "    AND dclocal_read_repair_chance = 0.1\n" +
            "    AND default_time_to_live = 7776000\n" +
            "    AND gc_grace_seconds = 864000\n" +
            "    AND max_index_interval = 2048\n" +
            "    AND memtable_flush_period_in_ms = 0\n" +
            "    AND min_index_interval = 128\n" +
            "    AND read_repair_chance = 0.0\n" +
            "    AND speculative_retry = '99PERCENTILE';";

    /**
     * INSERT statement to bulk load.
     * It is like prepared statement. You fill in place holder for each data.
     */
    public static final String INSERT_PROFILES_STMT = "INSERT INTO mappings.profiles (" +
            "marker,ssp,id" +
            ") VALUES (" +
            "?, ?, ?" +
            ")";

    public static final String INSERT_SEGMENTS_STMT = "INSERT INTO mappings.segments_mapping (" +
            "marker,segment,\"sourceId\",type" +
            ") VALUES (" +
            "?, ?, ?, ?" +
            ")";

    public static void main(String[] args)
    {
        if (args.length == 0)
        {
            System.out.println("usage: java bulkload.BulkLoad <list of files>");
            return;
        }

        // magic!
        Config.setClientMode(true);

        // Create output directory that has keyspace and table name in the path
        File profilesDir = new File(DEFAULT_OUTPUT_DIR + File.separator + KEYSPACE + File.separator + "profiles" + File.separator);
        File segmentsDir = new File(DEFAULT_OUTPUT_DIR + File.separator + KEYSPACE + File.separator + "segments_mapping" + File.separator);


        if (!profilesDir.exists() && !profilesDir.mkdirs())
        {
            throw new RuntimeException("Cannot create output directory: " + profilesDir);
        }
        if (!segmentsDir.exists() && !segmentsDir.mkdirs())
        {
            throw new RuntimeException("Cannot create output directory: " + segmentsDir);
        }

        // Prepare SSTable writer
        CQLSSTableWriter.Builder profilesBuilder = CQLSSTableWriter.builder();
        // set output directory
        profilesBuilder.inDirectory(profilesDir)
                // set target schema
                .forTable(PROFILES_SCHEMA)
                // set CQL statement to put data
                .using(INSERT_PROFILES_STMT)
                // set partitioner if needed
                // default is Murmur3Partitioner so set if you use different one.
                .withPartitioner(new Murmur3Partitioner());
        CQLSSTableWriter profilesWriter = profilesBuilder.build();

        // Prepare SSTable writer
        CQLSSTableWriter.Builder segmentsBuilder = CQLSSTableWriter.builder();
        // set output directory
        segmentsBuilder.inDirectory(segmentsDir)
                // set target schema
                .forTable(SEGMENTS_SCHEMA)
                // set CQL statement to put data
                .using(INSERT_SEGMENTS_STMT)
                // set partitioner if needed
                // default is Murmur3Partitioner so set if you use different one.
                .withPartitioner(new Murmur3Partitioner());
        CQLSSTableWriter segmentsWriter = segmentsBuilder.build();
int i = 0;
        for (String file : args)
        {
            try (BufferedReader reader = new BufferedReader(System.in))
            {
                // Write to SSTable while reading data
                String line;
                while ((line = reader.readLine()) != null)
                {
                    try {
                        JSONObject obj = new JSONObject(line);
                        i++;
                        String action = obj.getString("action");
                        String marker = obj.getString("marker");
                        String sourceId = obj.getString("sourceId");
                        String userId = obj.getString("userId");
                        String segment = obj.getString("segment");

                        if (action.equals("/user/sync/ssps") == true) {
                            profilesWriter.addRow(marker, sourceId, userId);
                        } else if (action.equals("/user/sync/dsps") == true) {
                            segmentsWriter.addRow(marker, sourceId, userId, "dsp");
                        }

                        if (segment.equals("") == false) {
                            segmentsWriter.addRow(marker, sourceId, segment, "segment");
                        }
                    } catch (org.json.JSONException e) {
                        System.out.println(line);
                        e.printStackTrace();
                    }
                }
            }
            catch (InvalidRequestException | IOException e)
            {
                e.printStackTrace();
            }
        }

        try
        {
            profilesWriter.close();
            segmentsWriter.close();
        }
        catch (IOException ignore) {}

        System.out.println(i);
    }
}
