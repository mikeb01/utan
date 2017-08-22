package com.lmax.utan.io;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class DirsTest
{
    @Test
    public void shouldFindNextDirectory() throws Exception
    {
        File dir = Dirs.createTempDir("FileUtilsTest");

        List<String> subdirs =
            ThreadLocalRandom.current()
                .ints(70, 1, 100)
                .mapToObj(i -> String.format("%02d", i))
                .sorted()
                .distinct()
                .peek(s -> createNewFile(dir, s))
                .collect(toList());

        for (int i = 0; i < subdirs.size(); i++)
        {
            String subdirName = subdirs.get(i);
            Predicate<String> valid = s -> s.matches("[0-9]{2}");
            File file = Dirs.nextSibling(new File(dir, subdirName), valid);
            if (i == subdirs.size() - 1)
            {
                assertThat(file).isNull();
            }
            else
            {
                assertThat(file.getName()).isEqualTo(subdirs.get(i + 1));
            }
        }
    }

    private void createNewFile(File dir, String s)
    {
        File f = new File(dir, s);
        try
        {
            if (!f.createNewFile())
            {
                throw new IOException("Unable to create file: " + f);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}