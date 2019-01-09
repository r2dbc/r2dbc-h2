package io.r2dbc.h2.client;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ClientTest {
    @Test
    void checkSqlPattern() {
        String selectStatement = "select * from dual";
        String insertStatement = "insert into test(id) values(1)";
        String insertIntoSelectStatement = "insert into test(id) select id from ids where 1 = 1";

        assertThat(Client.SELECT.matcher(selectStatement).matches()).isTrue();
        assertThat(Client.SELECT.matcher(insertStatement).matches()).isFalse();
        assertThat(Client.INSERT.matcher(insertStatement).matches()).isTrue();
        assertThat(Client.INSERT.matcher(selectStatement).matches()).isFalse();
        assertThat(Client.INSERT.matcher(insertIntoSelectStatement).matches()).isTrue();
        assertThat(Client.SELECT.matcher(insertIntoSelectStatement).matches()).isFalse();
    }

    @Test
    void checkSqlPatternWithWhitespace() {
        String selectStatement = "\r\n\t select\r\n\t *\r\n\t from\n\n dual \r\n\t";
        String insertStatement = "\n\n\t insert\r\n\t into\r\n\t test(id) values(1)\r\n\t";

        assertThat(Client.SELECT.matcher(selectStatement).matches()).isTrue();
        assertThat(Client.SELECT.matcher(insertStatement).matches()).isFalse();
        assertThat(Client.INSERT.matcher(insertStatement).matches()).isTrue();
        assertThat(Client.INSERT.matcher(selectStatement).matches()).isFalse();
    }
}