package org.apache.eventmesh.runtime.processor;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.apache.eventmesh.runtime.core.protocol.http.processor.WebHookProcessor;

import org.apache.http.HttpEntity;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.util.EntityUtils;

import java.nio.charset.StandardCharsets;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;

public class WebHookProcessorTest {

    @Test
    public void testHandler() {
        WebHookProcessor processor = new WebHookProcessor();
        FullHttpRequest req = new DefaultFullHttpRequest(HTTP_1_1, HttpMethod.GET, "/webhook/github/eventmesh/all");
        req.setMethod(HttpMethod.POST);
        ByteBuf byteBuf = Unpooled.copiedBuffer("\"test\": 1", StandardCharsets.UTF_8);
        req.content().writeBytes(byteBuf);
        processor.handler(req);
    }
}
