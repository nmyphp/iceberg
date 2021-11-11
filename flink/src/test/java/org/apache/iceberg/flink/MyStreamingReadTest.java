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

package org.apache.iceberg.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.source.FlinkSource;
import org.junit.Test;

public class MyStreamingReadTest {
    @Test
    public void read() {
        try {
            System.setProperty("dfs.nameservices", "xmanhdfs3");
            System.setProperty("dfs.ha.namenodes.xmanhdfs3", "nn1,nn2");
            System.setProperty("dfs.namenode.rpc-address.xmanhdfs3.nn1", "tob23.bigdata.lycc.qihoo.net:9000");
            System.setProperty("dfs.namenode.rpc-address.xmanhdfs3.nn2", "tob24.bigdata.lycc.qihoo.net:9000");

            StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

            String tablePath = "hdfs://tob23.bigdata.lycc.qihoo.net:9000/home/iceberg/warehouse/test/sample03";
            TableLoader tableLoader = TableLoader.fromHadoopTable(tablePath);
            DataStream<RowData> stream = FlinkSource.forRowData()
                    .env(env)
                    .tableLoader(tableLoader)
                    .streaming(true)
                    .startSnapshotId(7068043229986943802L)
                    .build();

            // Print all records to stdout.
            stream.print();

            // Submit and execute this streaming read job.
            env.execute("Test Iceberg Batch Read");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
