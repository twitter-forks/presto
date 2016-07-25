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
package com.facebook.presto.spi;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;
import static com.facebook.presto.spi.ErrorType.INSUFFICIENT_RESOURCES;
import static com.facebook.presto.spi.ErrorType.INTERNAL_ERROR;
import static com.facebook.presto.spi.ErrorType.USER_ERROR;

public enum StandardErrorCode
        implements ErrorCodeSupplier
{
    GENERIC_USER_ERROR(0x0000_0000, USER_ERROR),
    SYNTAX_ERROR(0x0000_0001, USER_ERROR),
    ABANDONED_QUERY(0x0000_0002, USER_ERROR),
    USER_CANCELED(0x0000_0003, USER_ERROR),
    PERMISSION_DENIED(0x0000_0004, USER_ERROR),
    NOT_FOUND(0x0000_0005, USER_ERROR),
    FUNCTION_NOT_FOUND(0x0000_0006, USER_ERROR),
    INVALID_FUNCTION_ARGUMENT(0x0000_0007, USER_ERROR),             // caught by TRY
    DIVISION_BY_ZERO(0x0000_0008, USER_ERROR),                      // caught by TRY
    INVALID_CAST_ARGUMENT(0x0000_0009, USER_ERROR),                 // caught by TRY
    OPERATOR_NOT_FOUND(0x0000_000A, USER_ERROR),
    INVALID_VIEW(0x0000_000B, USER_ERROR),
    ALREADY_EXISTS(0x0000_000C, USER_ERROR),
    NOT_SUPPORTED(0x0000_000D, USER_ERROR),
    INVALID_SESSION_PROPERTY(0x0000_000E, USER_ERROR),
    INVALID_WINDOW_FRAME(0x0000_000F, USER_ERROR),
    CONSTRAINT_VIOLATION(0x0000_0010, USER_ERROR),
    TRANSACTION_CONFLICT(0x0000_0011, USER_ERROR),
    INVALID_TABLE_PROPERTY(0x0000_0012, USER_ERROR),
    NUMERIC_VALUE_OUT_OF_RANGE(0x0000_0013, USER_ERROR),            // caught by TRY
    UNKNOWN_TRANSACTION(0x0000_0014, USER_ERROR),
    NOT_IN_TRANSACTION(0x0000_0015, USER_ERROR),
    TRANSACTION_ALREADY_ABORTED(0x0000_0016, USER_ERROR),
    READ_ONLY_VIOLATION(0x0000_0017, USER_ERROR),
    MULTI_CATALOG_WRITE_CONFLICT(0x0000_0018, USER_ERROR),
    AUTOCOMMIT_WRITE_CONFLICT(0x0000_0019, USER_ERROR),
    UNSUPPORTED_ISOLATION_LEVEL(0x0000_001A, USER_ERROR),
    INCOMPATIBLE_CLIENT(0x0000_001B, USER_ERROR),
    SUBQUERY_MULTIPLE_ROWS(0x0000_001C, USER_ERROR),
    PROCEDURE_NOT_FOUND(0x0000_001D, USER_ERROR),
    INVALID_PROCEDURE_ARGUMENT(0x0000_001E, USER_ERROR),
    QUERY_REJECTED(0x0000_001F, USER_ERROR),
    AMBIGUOUS_FUNCTION_CALL(0x0000_0020, USER_ERROR),

    GENERIC_INTERNAL_ERROR(0x0001_0000, INTERNAL_ERROR),
    TOO_MANY_REQUESTS_FAILED(0x0001_0001, INTERNAL_ERROR),
    PAGE_TOO_LARGE(0x0001_0002, INTERNAL_ERROR),
    PAGE_TRANSPORT_ERROR(0x0001_0003, INTERNAL_ERROR),
    PAGE_TRANSPORT_TIMEOUT(0x0001_0004, INTERNAL_ERROR),
    NO_NODES_AVAILABLE(0x0001_0005, INTERNAL_ERROR),
    REMOTE_TASK_ERROR(0x0001_0006, INTERNAL_ERROR),
    COMPILER_ERROR(0x0001_0007, INTERNAL_ERROR),
    REMOTE_TASK_MISMATCH(0x0001_0008, INTERNAL_ERROR),
    SERVER_SHUTTING_DOWN(0x0001_0009, INTERNAL_ERROR),
    FUNCTION_IMPLEMENTATION_MISSING(0x0001_000A, INTERNAL_ERROR),
    REMOTE_BUFFER_CLOSE_FAILED(0x0001_000B, INTERNAL_ERROR),
    SERVER_STARTING_UP(0x0001_000C, INTERNAL_ERROR),
    FUNCTION_IMPLEMENTATION_ERROR(0x0001_000D, INTERNAL_ERROR),
    INVALID_PROCEDURE_DEFINITION(0x0001_000E, INTERNAL_ERROR),
    PROCEDURE_CALL_FAILED(0x0001_000F, INTERNAL_ERROR),
    AMBIGUOUS_FUNCTION_IMPLEMENTATION(0x0001_0010, INTERNAL_ERROR),
    ABANDONED_TASK(0x0001_0011, INTERNAL_ERROR),

    GENERIC_INSUFFICIENT_RESOURCES(0x0002_0000, INSUFFICIENT_RESOURCES),
    EXCEEDED_MEMORY_LIMIT(0x0002_0001, INSUFFICIENT_RESOURCES),
    QUERY_QUEUE_FULL(0x0002_0002, INSUFFICIENT_RESOURCES),
    EXCEEDED_TIME_LIMIT(0x0002_0003, INSUFFICIENT_RESOURCES),
    CLUSTER_OUT_OF_MEMORY(0x0002_0004, INSUFFICIENT_RESOURCES),
    EXCEEDED_CPU_LIMIT(0x0002_0005, INSUFFICIENT_RESOURCES),

    // Connectors can use error codes starting at EXTERNAL
    // See https://github.com/prestodb/presto/wiki/Error-Codes
    GENERIC_EXTERNAL(0x0100_0000, EXTERNAL);

    private final ErrorCode errorCode;

    StandardErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
