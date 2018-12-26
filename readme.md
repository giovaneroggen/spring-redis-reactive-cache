## Redis cacheable support

Spring boot dependecies 2.1.0.RELEASE

###### Default redis properties
```
spring.redis.host=localhost
spring.redis.port=6379
```

#### Usage

Required dependencies for usage

    compile('io.projectreactor:reactor-core') or compile('org.springframework.boot:spring-boot-starter-webflux')
    compile('org.springframework.boot:spring-boot-starter-data-redis-reactive')

In order to use cache with reactive redis integration use both annotations

`br.com.roggen.cache.redis.@EnableCaching`

`br.com.roggen.cache.redis.@Cacheable`

`br.com.roggen.cache.redis.@CacheEvict`

#### Spring starter AOP is provided at this project

    compile('org.springframework.boot:spring-boot-starter-aop')
    
#### clean build publishToMavenLocal
