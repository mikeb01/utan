package com.lmax.utan.store;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.concurrent.ThreadLocalRandom;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class TimeSeriesStoreTest
{
    @Test
    public void shouldGetDay() throws Exception
    {
        final Clock clock = Clock.systemUTC();

        for (int i = 0; i < 20_000; i++)
        {
            int year = ThreadLocalRandom.current().nextInt(1970, 2020);
            int month = ThreadLocalRandom.current().nextInt(1, 13);
            int day = ThreadLocalRandom.current().nextInt(1, 28);

            final ZonedDateTime day1 = ZonedDateTime.of(year, month, day, 23, 59, 59, 999_999_999, clock.getZone());
            final ZonedDateTime day2 = day1.plusNanos(1);
            final long l1 = day1.toInstant().toEpochMilli();
            final long l2 = day2.toInstant().toEpochMilli();


//            System.out.printf("%s: %d, %d, %s%n", day1, TimeSeriesStore.getDay(l1), l1 % (24*60*60*1000), day1.toLocalDate());
//            System.out.printf("%s: %d, %d, %s%n", day2, TimeSeriesStore.getDay(l2), l2 % (24*60*60*1000), day2.toLocalDate());

            assertThat(TimeSeriesStore.getDay(l1), is(TimeSeriesStore.getDay(l2) - 1));
            assertThat(day1.toLocalDate().toEpochDay(), is(TimeSeriesStore.getDay(day1.toInstant().toEpochMilli())));
        }


        final ZonedDateTime day1 = ZonedDateTime.of(1970, 1, 1, 23, 59, 59, 999_999_999, clock.getZone());
        final ZonedDateTime day2 = ZonedDateTime.of(2070, 1, 1, 23, 59, 59, 999_999_999, clock.getZone());

        System.out.printf("%s: %d%n", day1, TimeSeriesStore.getDay(day1.toInstant().toEpochMilli()));
        System.out.printf("%s: %d%n", day2, TimeSeriesStore.getDay(day2.toInstant().toEpochMilli()));
    }
}