package br.com.roggen.cache.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.api.StatefulConnection;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Aspect
@Configuration
@ConditionalOnClass({ProceedingJoinPoint.class, MethodSignature.class,
                     ReactiveRedisTemplate.class, RedisConnectionFactory.class,
                     LettuceConnectionFactory.class, ReactiveRedisConnectionFactory.class})
public class CacheableAspect {

    @Autowired
    private ReactiveRedisTemplate<String, String> redisTemplate;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private ReactiveRedisConnection reactiveRedisConnection;
    @Value("${spring.application.name}")
    private String applicationName;


    @Around("execution(public * *(..)) && @annotation(Cacheable)")
    public Object logExecutionTime(ProceedingJoinPoint joinPoint) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        Cacheable annotation = method.getAnnotation(Cacheable.class);
        String name = annotation.name();
        String key = applicationName + ':' +  name + ':' + Arrays.hashCode(joinPoint.getArgs());
        Class<?> returnType = method.getReturnType();
        if (returnType.isAssignableFrom(Flux.class)) {
            return this.redisIsOpen()
                    .flatMapMany(it -> {
                        if (Boolean.TRUE.equals(it)) {
                            return redisTemplate.hasKey(key)
                                    .flatMapMany(found -> {
                                        if (found) {
                                            return redisTemplate.opsForValue()
                                                    .get(key)
                                                    .map(it2 -> {
                                                        try {
                                                            return objectMapper.readValue(it2, List.class);
                                                        } catch (IOException e) {
                                                            throw new RuntimeException(e);
                                                        }
                                                    }).flatMapMany(Flux::fromIterable);
                                        } else {
                                            try {
                                                List l = new ArrayList<>();
                                                return ((Flux) joinPoint.proceed())
                                                        .doOnEach((value) -> l.add(((Signal) value).get()))
                                                        .concatMap(v -> {
                                                            try {
                                                                return redisTemplate.opsForValue()
                                                                        .set(key, objectMapper.writeValueAsString(l))
                                                                        .map(saveResult -> {
                                                                            if (saveResult) {
                                                                                return v;
                                                                            } else {
                                                                                throw new RuntimeException(key + "NOT SAVED");
                                                                            }
                                                                        });
                                                            } catch (JsonProcessingException e) {
                                                                throw new RuntimeException(e);
                                                            }
                                                        });
                                            } catch (Throwable e) {
                                                throw new RuntimeException(e);
                                            }
                                        }
                                    });
                        } else {
                            try {
                                return (Flux) joinPoint.proceed();
                            } catch (Throwable e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });


        } else if (returnType.isAssignableFrom(Mono.class)) {
            return this.redisIsOpen()
                    .flatMap(it -> {
                        if (Boolean.TRUE.equals(it)) {
                            return redisTemplate.hasKey(key)
                                    .flatMap(foundValue -> {
                                        if (foundValue) {
                                            return redisTemplate.opsForValue()
                                                    .get(key)
                                                    .map(it2 -> {
                                                        try {
                                                            return objectMapper.readValue(it2, Object.class);
                                                        } catch (IOException e) {
                                                            throw new RuntimeException(e);
                                                        }
                                                    });
                                        } else {
                                            try {
                                                return ((Mono) joinPoint.proceed())
                                                        .delayUntil(v -> {
                                                            try {
                                                                return redisTemplate.opsForValue()
                                                                        .set(key, objectMapper.writeValueAsString(v))
                                                                        .map(saveResult -> {
                                                                            if (saveResult) {
                                                                                return v;
                                                                            } else {
                                                                                throw new RuntimeException(key + "NOT SAVED");
                                                                            }
                                                                        });
                                                            } catch (JsonProcessingException e) {
                                                                throw new RuntimeException(e);
                                                            }
                                                        });
                                            } catch (Throwable e) {
                                                throw new RuntimeException(e);
                                            }
                                        }
                                    });
                        } else {
                            try {
                                return (Mono) joinPoint.proceed();
                            } catch (Throwable e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
        } else {
            throw new RuntimeException("non reactive object supported (Mono, Flux)");
        }
    }
    private Mono<Boolean> redisIsOpen() throws NoSuchFieldException, IllegalAccessException {
        Field field = reactiveRedisConnection.getClass().getDeclaredField("sharedConnection");
        field.setAccessible(true);
        Mono<StatefulConnection<ByteBuffer, ByteBuffer>> sharedConnection = (Mono<StatefulConnection<ByteBuffer, ByteBuffer>>) field.get(this.reactiveRedisConnection);
        return sharedConnection
                .map(StatefulConnection::isOpen);
    }
}
