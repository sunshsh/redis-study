package com.example.redisdemo;

import com.example.redisdemo.redis.pojo.User;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.deploy.net.JARSigningException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

@SpringBootTest
class RedisDemoApplicationTests {
    @Autowired
//    private RedisTemplate<String, Object> redisTemplate;
    private StringRedisTemplate stringRedisTemplate;
    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    void contextLoads() throws JsonProcessingException {
//        redisTemplate.opsForValue().set("name", "Jack");
//        redisTemplate.opsForValue().set("user:100", new User("Jack", 21));
        // 创建对象
        User user = new User("Jack", 21);
        // 手动序列化
        String json = mapper.writeValueAsString(user);
        // 导入数据
        stringRedisTemplate.opsForValue().set("user:200", json);
        // 获取数据
        String jsonUser = stringRedisTemplate.opsForValue().get("user:200");
        // 手动反序列化
        User user1 = mapper.readValue(jsonUser, User.class);
        System.out.println(user1);
    }

}
