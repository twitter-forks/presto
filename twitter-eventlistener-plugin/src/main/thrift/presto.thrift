namespace java com.twitter.presto.thriftjava
#@namespace scala com.twitter.presto.thriftscala

enum QueryState {
    QUEUED    = 1,
    PLANNING  = 2,
    STARTING  = 3,
    RUNNING   = 4,
    FINISHING = 5,
    FINISHED  = 6,
    FAILED    = 7
}

struct OperatorStats {
    1: required i32 pipeline_id
    2: required i32 operator_id
    3: required string plan_node_id
    4: required string operator_type
    5: required i64 total_drivers
    6: required i64 add_input_calls
    7: required i64 add_input_wall_millis
    8: required i64 add_input_cpu_millis
    9: required i64 add_input_user_millis
    10: required i64 input_data_size_bytes
    11: required i64 input_positions
    12: required double sum_squared_input_positions
    13: required i64 get_output_calls
    14: required i64 get_output_wall_millis
    15: required i64 get_output_cpu_millis
    16: required i64 get_output_user_millis
    17: required i64 output_data_size_bytes
    18: required i64 output_positions
    19: required i64 blocked_wall_millis
    20: required i64 finish_calls
    21: required i64 finish_wall_millis
    22: required i64 finish_cpu_millis
    23: required i64 finish_user_millis
    24: required i64 memory_reservation_bytes
    25: required i64 system_memory_reservation_bytes
}(persisted='true')

struct QueryStageInfo {
    1: required i32 stage_id
    2: required i64 raw_input_data_size_bytes
    3: required i64 output_data_size_bytes
    4: required i32 completed_tasks
    5: required i32 completed_drivers
    6: required double cumulative_memory
    7: required i64 peak_memory_reservation_bytes
    8: required i64 total_scheduled_time_millis
    9: required i64 total_cpu_time_millis
    10: required i64 total_user_time_millis
    11: required i64 total_blocked_time_millis
}(persisted='true')

/**
 * Thrift version of a Presto QueryCompletionEvent. See QueryCompletionEvent for usage.
 */
struct QueryCompletionEvent {
    1: required string query_id
    2: optional string transaction_id
    3: required string user
    4: optional string principal
    5: optional string source
    6: optional string server_version
    7: optional string environment
    8: optional string catalog
    9: optional string schema
    10: optional string remote_client_address
    11: optional string user_agent
    12: required QueryState query_state
    13: optional string uri
    14: optional list<string> field_names
    15: required string query
    16: required i64 create_time_ms
    17: required i64 execution_start_time_ms
    18: required i64 end_time_ms
    19: required i64 queued_time_ms
    20: optional i64 analysis_time_ms
    21: required i64 distributed_planning_time_ms
    22: required i64 total_split_wall_time_ms
    23: required i64 total_split_cpu_time_ms
    24: required i64 total_bytes
    25: required i64 total_rows
    26: required i32 splits
    27: optional i32 error_code_id
    28: optional string error_code_name
    29: optional string failure_type
    30: optional string failure_message
    31: optional string failure_task
    32: optional string failure_host
    33: optional string output_stage_json
    34: optional string failures_json
    35: optional string inputs_json
    36: optional string session_properties_json

    # precalcuate some derived data to simplify queries
    200: required i64 query_wall_time_ms
    201: required i64 bytes_per_sec
    202: required i64 bytes_per_cpu_sec
    203: required i64 rows_per_sec
    204: required i64 rows_per_cpu_sec

    205: optional map<string, list<string>> queried_columns_by_table
    206: optional map<i32, QueryStageInfo> query_stages
    207: optional list<OperatorStats> operator_summaries

    208: optional i64 peak_memory_bytes
    209: optional double cumulative_memory_bytesecond
    210: optional i64 cpu_time_ms
}(persisted='true')
