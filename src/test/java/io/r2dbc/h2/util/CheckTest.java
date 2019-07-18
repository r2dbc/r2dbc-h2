package io.r2dbc.h2.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class CheckTest {
    @Test
    void findClass() {
        assertThat(Check.findClass("java.lang.Boolean")).isTrue();
    }

    @Test
    void findClassNotFound() {
        assertThat(Check.findClass("java.lang.Boolean123456789")).isFalse();
    }
}
