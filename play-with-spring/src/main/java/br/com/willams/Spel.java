package br.com.willams;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

class Foo {
    private final String name;

    Foo(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}

@Component
public class Spel {
    public Spel(@Value("*.#{foo.getName()}-*") String otherName) {
        System.out.println(otherName);
        this.otherName = otherName;
    }

    public String getOtherName() {
        return otherName;
    }






    private final String otherName;

}