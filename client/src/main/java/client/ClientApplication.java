package client;

import org.springframework.amqp.core.*;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.core.GenericTransformer;
import org.springframework.integration.dsl.DirectChannelSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.json.ObjectToJsonTransformer;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.Map;
import java.util.Set;

@IntegrationComponentScan
@SpringBootApplication
public class ClientApplication {

    public static void main(String[] args) {
        SpringApplication.run(ClientApplication.class, args);
    }

    private final String requests = "uppercase-requests";

    private final GenericHandler<Object> loggingHandler = (payload, headers) -> {
        System.out.println(new StringBuilder().repeat("-", 50));
        System.out.println(payload);
        for (var h : headers.keySet())
            System.out.println('\t' + h + '=' + headers.get(h));
        return payload;
    };

    private final GenericHandler<Object> terminatingLoggingHandler = (p, h) -> {
        this.loggingHandler.handle(p, h);
        return null;
    };

    @Bean
    DirectChannelSpec inbound() {
        return MessageChannels.direct();
    }

    @Bean
    DirectChannelSpec outbound() {
        return MessageChannels.direct();
    }

    @Bean
    IntegrationFlow outboundAmqpIntegrationFlow(AmqpTemplate amqpTemplate) {
        return IntegrationFlow
                .from(outbound())
                .transform((GenericTransformer<String, Map<String, String>>) source -> Map.of("request", source))
                .transform(new ObjectToJsonTransformer())
                .handle(Amqp.outboundGateway(amqpTemplate)
                        .routingKey(this.requests)
                        .exchangeName(this.requests)
                )
                .channel(inbound())
                .get();
    }

    @Bean
    ApplicationRunner starter(UppercaseClient uppercaseClient) {
        return args -> {
            System.out.println(uppercaseClient.uppercase("hi, world"));
            System.out.println(uppercaseClient.uppercase("moo"));

        };
    }

    @MessagingGateway
    public interface UppercaseClient {

        @Gateway(requestChannel = "outbound", replyChannel = "inbound")
        String uppercase(@Payload String input);
    }

    @Bean
    InitializingBean queueRegistrar(AmqpAdmin admin) {
        return () -> {
            for (var name : Set.of(this.requests)) {
                var q = QueueBuilder.durable(name).build();
                var e = ExchangeBuilder.directExchange(name).build();
                admin.declareQueue(q);
                admin.declareExchange(e);
                admin.declareBinding(BindingBuilder.bind(q).to(e).with(name).noargs());
            }
        };
    }

}



