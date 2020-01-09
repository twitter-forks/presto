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
package com.twitter.presto.gateway;

import com.facebook.presto.spi.security.Identity;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;

import java.util.Optional;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static java.util.Objects.requireNonNull;

public class RequestInfo
{
    private final String catalog;
    private final String schema;
    private final String source;
    private final Identity identity;

    private final String query;

    public RequestInfo(HttpServletRequest servletRequest, String query)
    {
        this.catalog = trimEmptyToNull(servletRequest.getHeader(PRESTO_CATALOG));
        this.schema = trimEmptyToNull(servletRequest.getHeader(PRESTO_SCHEMA));
        this.source = servletRequest.getHeader(PRESTO_SOURCE);
        this.identity = new Identity(
            trimEmptyToNull(servletRequest.getHeader(PRESTO_USER)),
            Optional.ofNullable(servletRequest.getUserPrincipal()));
        this.query = requireNonNull(query, "query is null");
    }

    @Nullable
    public String getCatalog()
    {
        return catalog;
    }

    @Nullable
    public String getSchema()
    {
        return schema;
    }

    @Nullable
    public String getSource()
    {
        return source;
    }

    @NotNull
    public Identity getIdentity()
    {
        return identity;
    }

    @NotNull
    public String getQuery()
    {
        return query;
    }

    private static String trimEmptyToNull(String value)
    {
        return emptyToNull(nullToEmpty(value).trim());
    }
}
