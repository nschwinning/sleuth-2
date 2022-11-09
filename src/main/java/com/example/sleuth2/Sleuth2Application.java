package com.example.sleuth2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@EnableAsync
@SpringBootApplication
public class Sleuth2Application {

    public static void main(String[] args) {
        SpringApplication.run(Sleuth2Application.class, args);
    }

}

@RequiredArgsConstructor
@RestController
class TestController {

    private final TestService testService;
    private Random random = new Random();

    @GetMapping("/api/test")
    public ResponseEntity<String> test(@RequestParam String message) throws Exception {

        //Thread.sleep(3000L);

        testService.handleMessage(message);

        return ResponseEntity.ok("Acknowledge");
    }

}

@Slf4j
@RequiredArgsConstructor
@Service
class TestService {

    private final KafkaTemplate<String, SimpleMessage> kafkaTemplate;

    private String topic = "simple";

    @Async
    public void handleMessage(String message) throws Exception {

        Thread.sleep(3000L);

        SimpleMessage simpleMessage = new SimpleMessage(message);
        ListenableFuture<SendResult<String,SimpleMessage>> sendResult = kafkaTemplate.send(topic, new SimpleMessage(message));
        sendResult.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(SendResult<String, SimpleMessage> result) {
                log.info("SimpleMessage [{}] delivered to topic {} with offset {}",
                        simpleMessage,
                        topic,
                        result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                log.warn("Unable to deliver SimpleMessage [{}] to topic {}. Exception is: {}",
                        simpleMessage,
                        topic,
                        ex.getMessage());
            }
        });

    }

}

@RequiredArgsConstructor
@Component
class ResponseHeaderFilter extends OncePerRequestFilter {

    private final Tracer tracer;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        response.addHeader("X-Correlation-Id", tracer.currentSpan().context().traceId());
        filterChain.doFilter(request, response);
    }

}

@Configuration
class KafkaConfiguration {

    private String bootstrapAddress = "localhost:9093";

    @Bean
    public KafkaTemplate<String, SimpleMessage> simpleMessageKafkaTemplate() {
        return new KafkaTemplate<>(simpleMessageProducerFactory());
    }

    @Bean
    public ProducerFactory<String, SimpleMessage> simpleMessageProducerFactory() {
        return new DefaultKafkaProducerFactory<>(getProducerProps());
    }

    private Map<String, Object> getProducerProps() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return producerProps;
    }

}

@Slf4j
@Aspect
@Component
class RestControllerLoggingAspect {

    @Pointcut("within(@org.springframework.web.bind.annotation.RestController *)")
    public void controllerInvocation() {}

    @Around("controllerInvocation()")
    public Object measureMethodExecutionTime(ProceedingJoinPoint pjp) throws Throwable {
        log.info("Rest Controller Invocation (ENTER) on method " + pjp.getSignature().getName());
        Object result = pjp.proceed();
        log.info("Rest Controller Invocation (EXIT) on method " + pjp.getSignature().getName());
        return result;
    }

}

@AllArgsConstructor
@Data
class SimpleMessage {

    private String message;

}