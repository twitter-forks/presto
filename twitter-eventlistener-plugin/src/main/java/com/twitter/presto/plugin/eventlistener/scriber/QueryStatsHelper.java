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
package com.twitter.presto.plugin.eventlistener.scriber;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.eventlistener.OperatorStatistics;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.twitter.presto.thriftjava.OperatorStats;
import com.twitter.presto.thriftjava.QueryStageInfo;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue.ValueType;

import java.io.StringReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;

public class QueryStatsHelper
{
    private static final Logger log = Logger.get(QueryStatsHelper.class);

    private QueryStatsHelper()
    {
        throw new AssertionError();
    }

    private static long getBytesOrNegativeOne(String strVal)
    {
        try {
            return DataSize.valueOf(strVal).toBytes();
        }
        catch (IllegalArgumentException e) {
            log.warn(e,
                    String.format("Failed to parse io.airlift.units.DataSize '%s', returning -1", strVal));
            return -1;
        }
    }

    private static long getMillisOrNegativeOne(String strVal)
    {
        try {
            return Duration.valueOf(strVal).toMillis();
        }
        catch (IllegalArgumentException e) {
            log.warn(e,
                    String.format("Failed to parse io.airlift.units.Duration '%s', returning -1", strVal));
            return -1;
        }
    }

    private static QueryStageInfo getQueryStageInfo(int stageId, JsonObject stage)
    {
        QueryStageInfo stageInfo = new QueryStageInfo();

        stageInfo.stage_id = stageId;
        try {
            JsonObject stageStats = stage.getJsonObject("latestAttemptExecutionInfo").getJsonObject("stats");
            stageInfo.raw_input_data_size_bytes = getBytesOrNegativeOne(stageStats.getString("rawInputDataSize"));
            stageInfo.output_data_size_bytes = getBytesOrNegativeOne(stageStats.getString("outputDataSize"));
            stageInfo.completed_tasks = stageStats.getInt("completedTasks");
            stageInfo.completed_drivers = stageStats.getInt("completedDrivers");
            stageInfo.cumulative_memory = stageStats.getJsonNumber("cumulativeUserMemory").doubleValue();
            stageInfo.peak_memory_reservation_bytes = getBytesOrNegativeOne(stageStats.getString("peakUserMemoryReservation"));
            stageInfo.total_scheduled_time_millis = getMillisOrNegativeOne(stageStats.getString("totalScheduledTime"));
            stageInfo.total_cpu_time_millis = getMillisOrNegativeOne(stageStats.getString("totalCpuTime"));
            stageInfo.total_blocked_time_millis = getMillisOrNegativeOne(stageStats.getString("totalBlockedTime"));
        }
        catch (Exception e) {
            log.error(e, String.format("Error retrieving stage stats for stage %d", stageId));
            return null;
        }

        return stageInfo;
    }

    private static OperatorStats getOperatorStat(OperatorStatistics stats)
    {
        OperatorStats operatorStats = new OperatorStats();

        try {
            operatorStats.pipeline_id = stats.getPipelineId();
            operatorStats.operator_id = stats.getOperatorId();
            operatorStats.plan_node_id = stats.getPlanNodeId().toString();
            operatorStats.operator_type = stats.getOperatorType();
            operatorStats.total_drivers = stats.getTotalDrivers();
            operatorStats.add_input_calls = stats.getAddInputCalls();
            operatorStats.add_input_wall_millis = stats.getAddInputWall().toMillis();
            operatorStats.add_input_cpu_millis = stats.getAddInputCpu().toMillis();
            operatorStats.input_data_size_bytes = stats.getInputDataSize().toBytes();
            operatorStats.input_positions = stats.getInputPositions();
            operatorStats.sum_squared_input_positions = stats.getSumSquaredInputPositions();
            operatorStats.get_output_calls = stats.getGetOutputCalls();
            operatorStats.get_output_wall_millis = stats.getGetOutputWall().toMillis();
            operatorStats.get_output_cpu_millis = stats.getGetOutputCpu().toMillis();
            operatorStats.output_data_size_bytes = stats.getOutputDataSize().toBytes();
            operatorStats.output_positions = stats.getOutputPositions();
            operatorStats.blocked_wall_millis = stats.getBlockedWall().toMillis();
            operatorStats.finish_calls = stats.getFinishCalls();
            operatorStats.finish_wall_millis = stats.getFinishWall().toMillis();
            operatorStats.finish_cpu_millis = stats.getFinishCpu().toMillis();
            operatorStats.memory_reservation_bytes = stats.getUserMemoryReservation().toBytes();
            operatorStats.system_memory_reservation_bytes = stats.getSystemMemoryReservation().toBytes();
        }
        catch (Exception e) {
            log.error(e, String.format("Error retrieving operator stats from JsonObject:\n%s\n", stats.toString()));
            return null;
        }

        return operatorStats;
    }

    public static Map<Integer, QueryStageInfo> getQueryStages(QueryMetadata eventMetadata)
    {
        if (!eventMetadata.getPayload().isPresent()) {
            return null;
        }

        String payload = eventMetadata.getPayload().get();
        Queue<JsonObject> stageJsonObjs = new LinkedList<>();
        try {
            JsonReader jsonReader = Json.createReader(new StringReader(payload));
            stageJsonObjs.add(jsonReader.readObject());
        }
        catch (Exception e) {
            log.error(e,
                    String.format("getQueryStages - Unable to extract JsonObject out of following blob:\n%s\n", payload));
            return null;
        }

        Map<Integer, QueryStageInfo> stages = new HashMap<>();
        while (!stageJsonObjs.isEmpty()) {
            JsonObject cur = stageJsonObjs.poll();
            String stageIdStr;
            try {
                stageIdStr = cur.getString("stageId");
                int stageId = Integer.parseInt(stageIdStr.split("\\.")[1]);
                QueryStageInfo curStage = getQueryStageInfo(stageId, cur);
                if (curStage != null) {
                    stages.put(stageId, getQueryStageInfo(stageId, cur));
                }
            }
            catch (Exception e) {
                log.error(e,
                        String.format("Failed to parse QueryStageInfo from JsonObject:\n%s\n", cur.toString()));
                return null;
            }

            try {
                cur.getJsonArray("subStages")
                        .stream()
                        .filter(val -> val.getValueType() == ValueType.OBJECT)
                        .forEach(val -> stageJsonObjs.add((JsonObject) val));
            }
            catch (Exception e) {
                log.error(e,
                        String.format("Failed to get subStages for stage %s, treating as no subStages", stageIdStr));
            }
        }

        return stages;
    }

    public static List<OperatorStats> getOperatorSummaries(List<OperatorStatistics> operatorStats)
    {
        try {
            return operatorStats
                    .stream()
                    .filter(val -> val != null)
                    .map(QueryStatsHelper::getOperatorStat)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }
        catch (Exception e) {
            log.error(e,
                    String.format("Error converting List<OperatorStatistics> to List<OperatorStats>:\n%s\n", operatorStats.toString()));
        }

        return null;
    }
}
