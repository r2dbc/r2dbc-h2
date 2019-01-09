package io.r2dbc.h2.client;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ClientTest {
    @Test
    void selectPattern() {
        String[] queries = {
                "select * from dual",
                "\r select \n* from dual",
                "\r\n select \n\n* from dual ",
                "\r\n select \n\n* from dual\n\r"
        };

        for (String query : queries) {
            assertThat(Client.SELECT.matcher(query).matches()).isTrue();
        }
    }

    @Test
    void insertPattern() {
        String[] queries = {
                "insert into test(id) values(1) ",
                "insert into test(id) select id from ids",
                "\rinsert into test(id) values(1) ",
                "\r\n\rinsert into test(id) values(1) ",
                "\r\n\rinsert \n into test(id) values(1)",
                "\r\n\rinsert \n\n into test(id) values(1) ",
                "\r\n\rinsert \n\n into test(id) values(1)\n\r",
                "\r\n\rinsert \n\n into test(id) select id from ids\n\r",
        };

        for (String query : queries) {
            assertThat(Client.INSERT.matcher(query).matches()).isTrue();
        }
    }
}