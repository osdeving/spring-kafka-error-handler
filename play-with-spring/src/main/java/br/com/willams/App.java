package br.com.willams;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class App implements CommandLineRunner {


    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    @Bean
    Foo foo() {
        return new Foo("joao");
    }

    @Override
    public void run(String... args) throws Exception {
    }
}