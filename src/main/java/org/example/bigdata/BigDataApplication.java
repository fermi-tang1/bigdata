package org.example.bigdata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;

/**
 * @Author: Fermi.Tang
 * @Date: Created in 14:48,2022/8/1
 */
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class, JpaRepositoriesAutoConfiguration.class, HibernateJpaAutoConfiguration.class})
public class BigDataApplication {
    public static void main(String[] args) {
        SpringApplication.run(BigDataApplication.class, args);
    }
}
