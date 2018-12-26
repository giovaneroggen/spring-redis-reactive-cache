package br.com.roggen.cache.redis;

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
import reactor.core.publisher.Mono;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;

@Aspect
@Configuration
@ConditionalOnClass({ProceedingJoinPoint.class, MethodSignature.class,
        ReactiveRedisTemplate.class, RedisConnectionFactory.class,
        LettuceConnectionFactory.class, ReactiveRedisConnectionFactory.class})
public class CacheEvictAspect {

    @Autowired
    private ReactiveRedisTemplate<String, String> redisTemplate;
    @Autowired
    private ReactiveRedisConnection reactiveRedisConnection;
    @Value("${spring.application.name}")
    private String applicationName;

    @Around("execution(public * *(..)) && @annotation(CacheEvict)")
    public Object logExecutionTime(ProceedingJoinPoint joinPoint) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        CacheEvict annotation = method.getAnnotation(CacheEvict.class);
        String name = annotation.name();
        String key = applicationName + ':' + name + ':' + Arrays.hashCode(joinPoint.getArgs());
        Class<?> returnType = method.getReturnType();
        if (returnType.isAssignableFrom(Mono.class)) {
            return this.redisIsOpen()
                    .flatMap(it -> {
                        if (Boolean.TRUE.equals(it)) {
                            return redisTemplate.hasKey(key)
                                    .flatMap(hasValue -> {
                                        if (hasValue) {
                                            return redisTemplate.opsForValue()
                                                    .delete(key)
                                                    .map(deleteResult -> {
                                                        if (deleteResult) {
                                                            return deleteResult;
                                                        } else {
                                                            throw new RuntimeException(key + "FAIL ON DELETE");
                                                        }
                                                    });
                                        } else {
                                            throw new RuntimeException(key + "NOT FOUND");
                                        }
                                    });
                        } else {
                            throw new RuntimeException("REDIS NOT AVAILABLE");
                        }
                    }).then();
        } else {
            throw new RuntimeException("Just Mono<Void> allowed for cacheEvict");
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
