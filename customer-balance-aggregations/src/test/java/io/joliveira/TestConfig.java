package io.joliveira;

import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TestConfig extends Config {

    @Bean
    public StreamingApp streamingApp() {
        return Mockito.mock(StreamingApp.class);
    }

}
