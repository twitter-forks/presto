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
package io.prestosql.plugin.druid;

import io.prestosql.plugin.druid.metadata.DruidSegmentInfo;
import io.prestosql.plugin.druid.segment.DruidSegmentReader;
import io.prestosql.plugin.druid.segment.HdfsDataInputSource;
import io.prestosql.plugin.druid.segment.IndexFileSource;
import io.prestosql.plugin.druid.segment.SegmentColumnSource;
import io.prestosql.plugin.druid.segment.SegmentIndexSource;
import io.prestosql.plugin.druid.segment.SmooshedColumnSource;
import io.prestosql.plugin.druid.segment.V9SegmentIndexSource;
import io.prestosql.plugin.druid.segment.ZipIndexFileSource;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

import static io.prestosql.plugin.druid.DruidErrorCode.DRUID_DEEP_STORAGE_ERROR;

public class DruidPageSourceProvider
        implements ConnectorPageSourceProvider
{
    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns)
    {
        DruidSplit druidSplit = (DruidSplit) split;

        // parse Druid segment locations
        DruidSegmentInfo segmentInfo = druidSplit.getSegmentInfo();

        DruidSegmentInfo.DeepStorageType type = segmentInfo.getDeepStorageType();
        try {
            Path hdfsPath = new Path(segmentInfo.getDeepStoragePath());
            FileSystem fileSystem = hdfsPath.getFileSystem(new Configuration());
            long fileSize = fileSystem.getFileStatus(hdfsPath).getLen();
            FSDataInputStream inputStream = fileSystem.open(hdfsPath);
            DataInputSourceId dataInputSourceId = new DataInputSourceId(hdfsPath.toString());
            HdfsDataInputSource dataInputSource = new HdfsDataInputSource(dataInputSourceId, inputStream, fileSize);
            IndexFileSource indexFileSource = new ZipIndexFileSource(dataInputSource);
            SegmentColumnSource segmentColumnSource = new SmooshedColumnSource(indexFileSource);
            SegmentIndexSource segmentIndexSource = new V9SegmentIndexSource(segmentColumnSource);

            return new DruidSegmentPageSource(
                    druidSplit.getSegmentInfo(),
                    columns,
                    new DruidSegmentReader(segmentIndexSource, columns));
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_DEEP_STORAGE_ERROR, e);
        }
    }
}
