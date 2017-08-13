package com.lmax.utan.store;

import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;

import static com.lmax.io.Dirs.ensureDirExists;
import static org.agrona.BitUtil.toHex;

public class PersistentStore
{
    static File getKeyDir(byte[] keyAsBytes, boolean createIfNotExists, MessageDigest sha1Digest, File parent) throws IOException
    {
        sha1Digest.reset();
        byte[] digest = sha1Digest.digest(keyAsBytes);

        String digestString = toHex(digest);
        String prefix = digestString.substring(0, 3);
        String suffix = digestString.substring(3);

        File keyPath = parent.toPath().resolve(prefix).resolve(suffix).toFile();

        if (createIfNotExists)
        {
            ensureDirExists(keyPath);
        }

        return keyPath;
    }
}
