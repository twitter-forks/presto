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
package com.twitter.presto.plugin.eventlistener;

import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;

import com.twitter.presto.thriftjava.OperatorStats;
import com.twitter.presto.thriftjava.QueryStageInfo;

import io.airlift.log.Logger;
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
            JsonObject stageStats = stage.getJsonObject("stageStats");
            stageInfo.raw_input_data_size_bytes = getBytesOrNegativeOne(stageStats.getString("rawInputDataSize"));
            stageInfo.output_data_size_bytes = getBytesOrNegativeOne(stageStats.getString("outputDataSize"));
            stageInfo.completed_tasks = stageStats.getInt("completedTasks");
            stageInfo.completed_drivers = stageStats.getInt("completedDrivers");
            stageInfo.cumulative_memory = stageStats.getJsonNumber("cumulativeMemory").doubleValue();
            stageInfo.peak_memory_reservation_bytes = getBytesOrNegativeOne(stageStats.getString("peakMemoryReservation"));
            stageInfo.total_scheduled_time_millis = getMillisOrNegativeOne(stageStats.getString("totalScheduledTime"));
            stageInfo.total_cpu_time_millis = getMillisOrNegativeOne(stageStats.getString("totalCpuTime"));
            stageInfo.total_user_time_millis = getMillisOrNegativeOne(stageStats.getString("totalUserTime"));
            stageInfo.total_blocked_time_millis = getMillisOrNegativeOne(stageStats.getString("totalBlockedTime"));
        }
        catch (Exception e) {
            log.error(e, String.format("Error retrieving stage stats for stage %d", stageId));
            return null;
        }

        return stageInfo;
    }

    private static OperatorStats getOperatorStat(String operatorSummaryStr)
    {
        try {
            JsonReader jsonReader = Json.createReader(new StringReader(operatorSummaryStr));
            return getOperatorStat(jsonReader.readObject());
        }
        catch (Exception e) {
            log.error(e, String.format("Error retrieving operator stats from string:\n%s\n", operatorSummaryStr));
        }

        return null;
    }

    private static OperatorStats getOperatorStat(JsonObject obj)
    {
        OperatorStats operatorStats = new OperatorStats();

        try {
            operatorStats.pipeline_id = obj.getInt("pipelineId");
            operatorStats.operator_id = obj.getInt("operatorId");
            operatorStats.plan_node_id = obj.getString("planNodeId");
            operatorStats.operator_type = obj.getString("operatorType");
            operatorStats.total_drivers = obj.getJsonNumber("totalDrivers").longValue();
            operatorStats.add_input_calls = obj.getJsonNumber("addInputCalls").longValue();
            operatorStats.add_input_wall_millis = getMillisOrNegativeOne(obj.getString("addInputWall"));
            operatorStats.add_input_cpu_millis = getMillisOrNegativeOne(obj.getString("addInputCpu"));
            operatorStats.add_input_user_millis = getMillisOrNegativeOne(obj.getString("addInputUser"));
            operatorStats.input_data_size_bytes = getBytesOrNegativeOne(obj.getString("inputDataSize"));
            operatorStats.input_positions = obj.getJsonNumber("inputPositions").longValue();
            operatorStats.sum_squared_input_positions = obj.getJsonNumber("sumSquaredInputPositions").doubleValue();
            operatorStats.get_output_calls = obj.getJsonNumber("getOutputCalls").longValue();
            operatorStats.get_output_wall_millis = getMillisOrNegativeOne(obj.getString("getOutputWall"));
            operatorStats.get_output_cpu_millis = getMillisOrNegativeOne(obj.getString("getOutputCpu"));
            operatorStats.get_output_user_millis = getMillisOrNegativeOne(obj.getString("getOutputUser"));
            operatorStats.output_data_size_bytes = getBytesOrNegativeOne(obj.getString("outputDataSize"));
            operatorStats.output_positions = obj.getJsonNumber("outputPositions").longValue();
            operatorStats.blocked_wall_millis = getMillisOrNegativeOne(obj.getString("blockedWall"));
            operatorStats.finish_calls = obj.getJsonNumber("finishCalls").longValue();
            operatorStats.finish_wall_millis = getMillisOrNegativeOne(obj.getString("finishWall"));
            operatorStats.finish_cpu_millis = getMillisOrNegativeOne(obj.getString("finishCpu"));
            operatorStats.finish_user_millis = getMillisOrNegativeOne(obj.getString("finishUser"));
            operatorStats.memory_reservation_bytes = getBytesOrNegativeOne(obj.getString("memoryReservation"));
            operatorStats.system_memory_reservation_bytes = getBytesOrNegativeOne(obj.getString("systemMemoryReservation"));
        }
        catch (Exception e) {
            log.error(e, String.format("Error retrieving operator stats from JsonObject:\n%s\n", obj.toString()));
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
        Queue<JsonObject> stageJsonObjs = new LinkedList<JsonObject>();
        try {
            JsonReader jsonReader = Json.createReader(new StringReader(payload));
            stageJsonObjs.add(jsonReader.readObject());
        }
        catch (Exception e) {
            log.error(e,
                    String.format("getQueryStages - Unable to extract JsonObject out of following blob:\n%s\n", payload));
            return null;
        }

        Map<Integer, QueryStageInfo> stages = new HashMap<Integer, QueryStageInfo>();
        while (!stageJsonObjs.isEmpty()) {
            JsonObject cur = stageJsonObjs.poll();
            String stageIdStr = "Unknown";
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

    public static List<OperatorStats> getOperatorSummaries(QueryStatistics eventStat)
    {
        try {
            return eventStat.getOperatorSummaries()
                    .stream()
                    .filter(val -> val != null && !val.isEmpty())
                    .map(QueryStatsHelper::getOperatorStat)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }
        catch (Exception e) {
            log.error(e,
                    String.format("Error converting List<String> to List<OperatorStats>:\n%s\n", eventStat.getOperatorSummaries().toString()));
        }

        return null;
    }
}
