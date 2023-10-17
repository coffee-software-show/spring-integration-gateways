package client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.*;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.core.GenericTransformer;
import org.springframework.integration.dsl.DirectChannelSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.json.ObjectToJsonTransformer;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

@IntegrationComponentScan
@RegisterReflectionForBinding (ClientApplication.UppercaseReply.class)
@SpringBootApplication
public class ClientApplication {

    public static void main(String[] args) {
        SpringApplication.run(ClientApplication.class, args);
    }

    private final String requests = "uppercase-requests";

    @Bean("inbound")
    DirectChannelSpec inbound() {
        return MessageChannels.direct();
    }

    @Bean("outbound")
    DirectChannelSpec outbound() {
        return MessageChannels.direct();
    }

    @Bean
    IntegrationFlow outboundAmqpIntegrationFlow(@Qualifier("outbound") MessageChannel outbound, @Qualifier("inbound") MessageChannel inbound, ObjectMapper objectMapper, AmqpTemplate amqpTemplate) {
        return IntegrationFlow
                .from(outbound)
                .transform((GenericTransformer<String, Map<String, String>>) source -> Map.of("request", source))
                .transform(new ObjectToJsonTransformer())
                .handle(Amqp.outboundGateway(amqpTemplate)
                        .routingKey(this.requests)
                        .exchangeName(this.requests)
                )
                .transform((GenericTransformer<String, UppercaseReply>) source -> {
                    try {
                        return objectMapper.readValue(source, UppercaseReply.class);
                    }//
                    catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                })
                .channel(inbound)
                .get();
    }

    @Bean
    ApplicationRunner starter(UppercaseClient uppercaseClient) {
        return args -> {

            record UppercasingRunnable(UppercaseClient uppercaseClient, String message) implements Runnable {
                @Override
                public void run() {
                    System.out.println(uppercaseClient.uppercase(message));
                }
            }

            var es = Executors.newVirtualThreadPerTaskExecutor();
            for (var i = 0; i < 100; i++)
                for (var message : "hello world,meow,moo,woof".split(","))
                    es.submit(new UppercasingRunnable(uppercaseClient, message));
        };
    }

    @MessagingGateway
    public interface UppercaseClient {

        @Gateway(requestChannel = "outbound", replyChannel = "inbound")
        UppercaseReply uppercase(@Payload String input);
    }

    public record UppercaseReply(String reply) {
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



