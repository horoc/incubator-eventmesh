/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eventmesh.protocol.cloudevents.resolver.grpc;

import org.apache.eventmesh.common.protocol.grpc.common.ProtocolKey;
import org.apache.eventmesh.common.protocol.grpc.common.SimpleMessageWrapper;
import org.apache.eventmesh.common.protocol.grpc.protos.BatchMessage;
import org.apache.eventmesh.common.protocol.grpc.protos.RequestHeader;
import org.apache.eventmesh.common.protocol.grpc.protos.SimpleMessage;

import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import io.cloudevents.CloudEvent;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;

public class GrpcMessageProtocolResolver {

    public static CloudEvent buildEvent(SimpleMessage message) {
        String cloudEventJson = message.getContent();

        String contentType = message.getPropertiesOrDefault(ProtocolKey.CONTENT_TYPE, "application/cloudevents+json");
        EventFormat eventFormat = EventFormatProvider.getInstance().resolveFormat(contentType);
        CloudEvent event = eventFormat.deserialize(cloudEventJson.getBytes(StandardCharsets.UTF_8));

        RequestHeader header = message.getHeader();
        CloudEventBuilder eventBuilder = buildFromRequestHeader(header, event);

        String seqNum = notEmptyOrDefault(message.getSeqNum(), getExtension(event, ProtocolKey.SEQ_NUM));
        String uniqueId = notEmptyOrDefault(message.getUniqueId(), getExtension(event, ProtocolKey.UNIQUE_ID));
        String topic = notEmptyOrDefault(message.getTopic(), event.getSubject());
        String ttl = notEmptyOrDefault(message.getTtl(), getExtension(event, ProtocolKey.TTL));
        String producerGroup = notEmptyOrDefault(message.getProducerGroup(), getExtension(event, ProtocolKey.PRODUCERGROUP));

        eventBuilder.withSubject(topic)
            .withExtension(ProtocolKey.SEQ_NUM, seqNum)
            .withExtension(ProtocolKey.UNIQUE_ID, uniqueId)
            .withExtension(ProtocolKey.PRODUCERGROUP, producerGroup)
            .withExtension(ProtocolKey.TTL, ttl);

        message.getPropertiesMap().forEach((k, v) -> eventBuilder.withExtension(k, v));

        return eventBuilder.build();
    }

    public static SimpleMessageWrapper buildSimpleMessage(CloudEvent cloudEvent) {
        String env = getExtensionOrDefault(cloudEvent, ProtocolKey.ENV, "env");
        String idc = getExtensionOrDefault(cloudEvent, ProtocolKey.IDC, "idc");
        String ip = getExtensionOrDefault(cloudEvent, ProtocolKey.IP, "127.0.0.1");
        String pid = getExtensionOrDefault(cloudEvent, ProtocolKey.PID, "123");
        String sys = getExtensionOrDefault(cloudEvent, ProtocolKey.SYS, "sys123");
        String userName = getExtensionOrDefault(cloudEvent, ProtocolKey.USERNAME, "user");
        String passwd = getExtensionOrDefault(cloudEvent, ProtocolKey.PASSWD, "pass");
        String language = getExtensionOrDefault(cloudEvent, ProtocolKey.LANGUAGE, "JAVA");
        String protocol = getExtensionOrDefault(cloudEvent, ProtocolKey.PROTOCOL_TYPE, "protocol");
        String protocolDesc = getExtensionOrDefault(cloudEvent, ProtocolKey.PROTOCOL_DESC, "protocolDesc");
        String protocolVersion = getExtensionOrDefault(cloudEvent, ProtocolKey.PROTOCOL_VERSION, "1.0");
        String seqNum = getExtensionOrDefault(cloudEvent, ProtocolKey.SEQ_NUM, "");
        String uniqueId = getExtensionOrDefault(cloudEvent, ProtocolKey.UNIQUE_ID, "");
        String producerGroup = getExtensionOrDefault(cloudEvent, ProtocolKey.PRODUCERGROUP, "producerGroup");
        String ttl = getExtensionOrDefault(cloudEvent, ProtocolKey.TTL, "3000");

        RequestHeader header = RequestHeader.newBuilder()
            .setEnv(env).setIdc(idc)
            .setIp(ip).setPid(pid)
            .setSys(sys).setUsername(userName).setPassword(passwd)
            .setLanguage(language).setProtocolType(protocol)
            .setProtocolDesc(protocolDesc).setProtocolVersion(protocolVersion)
            .build();

        String contentType = cloudEvent.getDataContentType();
        EventFormat eventFormat = EventFormatProvider.getInstance().resolveFormat(contentType);

        SimpleMessage.Builder messageBuilder = SimpleMessage.newBuilder()
            .setHeader(header)
            .setContent(new String(eventFormat.serialize(cloudEvent), StandardCharsets.UTF_8))
            .setProducerGroup(producerGroup)
            .setSeqNum(seqNum)
            .setUniqueId(uniqueId)
            .setTopic(cloudEvent.getSubject())
            .setTtl(ttl)
            .putProperties(ProtocolKey.CONTENT_TYPE, contentType);

        for (String key : cloudEvent.getExtensionNames()) {
            messageBuilder.putProperties(key, cloudEvent.getExtension(key).toString());
        }

        SimpleMessage simpleMessage = messageBuilder.build();

        return new SimpleMessageWrapper(simpleMessage);
    }

    public static List<CloudEvent> buildBatchEvents(BatchMessage batchMessage) {
        List<CloudEvent> cloudEvents = new ArrayList<>();

        RequestHeader header = batchMessage.getHeader();

        for (BatchMessage.MessageItem item : batchMessage.getMessageItemList()) {
            String cloudEventJson = item.getContent();

            String contentType = item.getPropertiesOrDefault(ProtocolKey.CONTENT_TYPE, "application/cloudevents+json");
            EventFormat eventFormat = EventFormatProvider.getInstance().resolveFormat(contentType);
            CloudEvent event = eventFormat.deserialize(cloudEventJson.getBytes(StandardCharsets.UTF_8));

            CloudEventBuilder eventBuilder = buildFromRequestHeader(header, event);

            String seqNum = notEmptyOrDefault(item.getSeqNum(), getExtension(event, ProtocolKey.SEQ_NUM));
            String uniqueId = notEmptyOrDefault(item.getUniqueId(), getExtension(event, ProtocolKey.UNIQUE_ID));
            String topic = notEmptyOrDefault(batchMessage.getTopic(), event.getSubject());
            String producerGroup = notEmptyOrDefault(batchMessage.getProducerGroup(), getExtension(event, ProtocolKey.PRODUCERGROUP));
            String ttl = notEmptyOrDefault(item.getTtl(), getExtension(event, ProtocolKey.TTL));

            eventBuilder.withSubject(topic)
                .withExtension(ProtocolKey.SEQ_NUM, seqNum)
                .withExtension(ProtocolKey.UNIQUE_ID, uniqueId)
                .withExtension(ProtocolKey.PRODUCERGROUP, producerGroup)
                .withExtension(ProtocolKey.TTL, ttl);

            item.getPropertiesMap().forEach((k, v) -> eventBuilder.withExtension(k, v));

            cloudEvents.add(eventBuilder.build());
        }
        return cloudEvents;
    }

    private static CloudEventBuilder buildFromRequestHeader(RequestHeader header, CloudEvent event) {
        String env = notEmptyOrDefault(header.getEnv(), getExtension(event, ProtocolKey.ENV));
        String idc = notEmptyOrDefault(header.getIdc(), getExtension(event, ProtocolKey.IDC));
        String ip = notEmptyOrDefault(header.getIp(), getExtension(event, ProtocolKey.IP));
        String pid = notEmptyOrDefault(header.getPid(), getExtension(event, ProtocolKey.PID));
        String sys = notEmptyOrDefault(header.getSys(), getExtension(event, ProtocolKey.SYS));
        String language = notEmptyOrDefault(header.getLanguage(), getExtension(event, ProtocolKey.LANGUAGE));
        String protocolType = notEmptyOrDefault(header.getProtocolType(), getExtension(event, ProtocolKey.PROTOCOL_TYPE));
        String protocolDesc = notEmptyOrDefault(header.getProtocolDesc(), getExtension(event, ProtocolKey.PROTOCOL_DESC));
        String protocolVersion = notEmptyOrDefault(header.getProtocolVersion(), getExtension(event, ProtocolKey.PROTOCOL_VERSION));
        String username = notEmptyOrDefault(header.getUsername(), getExtension(event, ProtocolKey.USERNAME));
        String passwd = notEmptyOrDefault(header.getPassword(), getExtension(event, ProtocolKey.PASSWD));

        CloudEventBuilder eventBuilder;
        if (StringUtils.equals(SpecVersion.V1.toString(), protocolVersion)) {
            eventBuilder = CloudEventBuilder.v1(event);
        } else {
            eventBuilder = CloudEventBuilder.v03(event);
        }
        eventBuilder.withExtension(ProtocolKey.ENV, env)
            .withExtension(ProtocolKey.IDC, idc)
            .withExtension(ProtocolKey.IP, ip)
            .withExtension(ProtocolKey.PID, pid)
            .withExtension(ProtocolKey.SYS, sys)
            .withExtension(ProtocolKey.USERNAME, username)
            .withExtension(ProtocolKey.PASSWD, passwd)
            .withExtension(ProtocolKey.LANGUAGE, language)
            .withExtension(ProtocolKey.PROTOCOL_TYPE, protocolType)
            .withExtension(ProtocolKey.PROTOCOL_DESC, protocolDesc)
            .withExtension(ProtocolKey.PROTOCOL_VERSION, protocolVersion);
        return eventBuilder;
    }

    private static String getExtensionOrDefault(CloudEvent cloudEvent, String extensionKey, String defaultValue) {
        if (cloudEvent != null) {
            Object value = cloudEvent.getExtension(extensionKey);
            return Objects.nonNull(value) && StringUtils.isNoneBlank(value.toString()) ? value.toString() : defaultValue;
        }
        return defaultValue;
    }

    private static String getExtension(CloudEvent cloudEvent, String extensionKey) {
        if (cloudEvent != null) {
            Object value = cloudEvent.getExtension(extensionKey);
            return Objects.nonNull(value) ? value.toString() : null;
        }
        return null;
    }

    private static String notEmptyOrDefault(String value, String defaultValue) {
        return StringUtils.isNotEmpty(value) ? value : defaultValue;
    }
}
