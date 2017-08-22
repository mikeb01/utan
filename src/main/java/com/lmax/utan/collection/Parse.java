package com.lmax.utan.collection;

public class Parse
{
    public static long parseLong(CharSequence s, int radix)
        throws NumberFormatException
    {
        if (s == null)
        {
            throw new NumberFormatException("null");
        }

        if (radix < Character.MIN_RADIX)
        {
            throw new NumberFormatException("radix " + radix +
                " less than Character.MIN_RADIX");
        }
        if (radix > Character.MAX_RADIX)
        {
            throw new NumberFormatException("radix " + radix +
                " greater than Character.MAX_RADIX");
        }

        long result = 0;
        boolean negative = false;
        int i = 0, len = s.length();
        long limit = -Long.MAX_VALUE;
        long multmin;
        int digit;

        if (len > 0)
        {
            char firstChar = s.charAt(0);
            if (firstChar < '0')
            { // Possible leading "+" or "-"
                if (firstChar == '-')
                {
                    negative = true;
                    limit = Long.MIN_VALUE;
                }
                else if (firstChar != '+')
                    throw new NumberFormatException("For input string: \"" + s + "\"");

                if (len == 1) // Cannot have lone "+" or "-"
                    throw new NumberFormatException("For input string: \"" + s + "\"");
                i++;
            }
            multmin = limit / radix;
            while (i < len)
            {
                // Accumulating negatively avoids surprises near MAX_VALUE
                digit = Character.digit(s.charAt(i++), radix);
                if (digit < 0)
                {
                    throw new NumberFormatException("For input string: \"" + s + "\"");
                }
                if (result < multmin)
                {
                    throw new NumberFormatException("For input string: \"" + s + "\"");
                }
                result *= radix;
                if (result < limit + digit)
                {
                    throw new NumberFormatException("For input string: \"" + s + "\"");
                }
                result -= digit;
            }
        }
        else
        {
            throw new NumberFormatException("For input string: \"" + s + "\"");
        }
        return negative ? result : -result;
    }
}
