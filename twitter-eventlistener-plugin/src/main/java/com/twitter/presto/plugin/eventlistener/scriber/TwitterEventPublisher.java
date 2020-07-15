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

import com.twitter.logpipeline.client.EventPublisherManager;
import com.twitter.logpipeline.client.common.EventLogMessage;
import com.twitter.logpipeline.client.common.EventPublisher;
import com.twitter.logpipeline.client.serializers.EventLogMsgTBinarySerializer;
import com.twitter.presto.thriftjava.QueryCompletionEvent;
import org.apache.thrift.TException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class TwitterEventPublisher
{
    private static final String gcpOrgName = "twttr-dp-org-ie";
    private static final String gcpCredentialPath = "/var/lib/tss/keys/presto/cloud/gcp/dp/shadow.json";

    private EventPublisher<QueryCompletionEvent> publisher;

    public TwitterEventPublisher(String scribeCategory)
    {
        // Build GCP Log Pipeline publisher
        String logCategoryName = "projects/" + gcpOrgName + "/topics/" + scribeCategory;
        this.publisher = EventPublisherManager.buildGcpLogPipelinePublisher(
                logCategoryName,
                EventLogMsgTBinarySerializer.getNewSerializer(),
                gcpCredentialPath);
    }

    public void scribe(QueryCompletionEvent thriftMessage)
            throws TException
    {
        // Build Event log message and publish the event asynchronously
        EventLogMessage<QueryCompletionEvent> message =
                EventLogMessage.buildEventLogMessage(publisher.getLogCategoryName(), thriftMessage);
        CompletableFuture<String> future = publisher.publish(message);
        if (future.isCompletedExceptionally()) {
            try {
                future.get();
            }
            catch (InterruptedException | ExecutionException e) {
                throw new TException(e);
            }
        }
    }
}
