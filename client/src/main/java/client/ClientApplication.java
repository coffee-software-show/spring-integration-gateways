package client;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.dsl.DirectChannelSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;

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
    DirectChannelSpec outbound() {
        return MessageChannels.direct();
    }

    @Bean
    DirectChannelSpec inbound() {
        return MessageChannels.direct();
    }

    @Bean
    IntegrationFlow requests(AmqpTemplate template) {

        return IntegrationFlow
                .from(outbound())
                .handle(this.loggingHandler)
                .handle(Amqp.outboundAdapter(template).routingKey(this.requests).exchangeName(this.requests))
                .get();

    }

    @Bean
    IntegrationFlow replies(ConnectionFactory connectionFactory) {
        return IntegrationFlow
                .from(Amqp.inboundAdapter(connectionFactory, this.replies))
                .handle(this.terminatingLoggingHandler)
                .get();
    }

    @Bean
    ApplicationRunner starter(MessageChannel outbound) {
        return args -> outbound.send(MessageBuilder.withPayload("hello, world").build());
    }

/*    @MessagingGateway
    interface UppercaseClient {

        @Gateway
        String uppercase (String lowercase);
    }

    @Bean
    IntegrationFlow outboundRequestsFlow (AmqpTemplate template) {

        var outboundAmqpGateway = Amqp
                .outboundGateway(template)
                .routingKey( this.q);
        return IntegrationFlow
                .from(new MessageSource<String>() {
                    @Override
                    public Message<String> receive() {
                        return null;
                    }
                })
                .gateway(  outboundAmqpGateway )
                .get() ;
    }*/


}



