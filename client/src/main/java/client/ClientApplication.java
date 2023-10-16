package client;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.dsl.DirectChannelSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.Payload;

@IntegrationComponentScan
@SpringBootApplication
public class ClientApplication {

    public static void main(String[] args) {
        SpringApplication.run(ClientApplication.class, args);
    }

    private final String requests = "uppercase-requests";

    private final String replies = "uppercase-replies";

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
                .handle(Amqp.outboundGateway(amqpTemplate)
                        .routingKey(this.requests)
                        .exchangeName(this.requests)
                )
                .channel(inbound())
                .get();
    }

    @Bean
    ApplicationRunner starter(Uppercase uppercase, MessageChannel outbound) {
        return args -> {
            System.out.println(uppercase.uppercase("hi, world"));
            System.out.println(uppercase.uppercase("moo"));

        };
    }

    @MessagingGateway
    public interface Uppercase {

        @Gateway(requestChannel = "outbound", replyChannel = "inbound")
        String uppercase(@Payload String input);
    }

}



