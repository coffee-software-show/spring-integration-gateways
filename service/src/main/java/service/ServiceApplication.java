package service;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.dsl.IntegrationFlow;

import java.util.Set;

@SpringBootApplication
public class ServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ServiceApplication.class, args);
    }

    private final String requests = "uppercase-requests";

    private final String replies = "uppercase-replies";

    @Bean
    IntegrationFlow requestsIntegrationFlow(ConnectionFactory connectionFactory) {
        return IntegrationFlow
                .from(Amqp.inboundGateway(connectionFactory, this.requests).defaultReplyTo(this.replies))
                .handle( this.loggingHandler)
                .handle((GenericHandler<String>) (payload, headers) -> {
                    var reply = payload.toUpperCase();
                    System.out.println(reply);
                    return reply;
                })
                .get();
    }

    @Bean
    InitializingBean queueRegistrar(AmqpAdmin admin) {
        return () -> {
            for (var name : Set.of(this.requests, this.replies)) {
                var q = QueueBuilder.durable(name).build();
                var e = ExchangeBuilder.directExchange(name).build();
                admin.declareQueue(q);
                admin.declareExchange(e);
                admin.declareBinding(BindingBuilder.bind(q).to(e).with(name).noargs());
            }
        };
    }


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

}