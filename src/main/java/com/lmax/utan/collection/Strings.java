package com.lmax.utan.collection;

public class Strings
{
    public static String lPad2(int value)
    {
        switch (value)
        {
            case  0: return "00";
            case  1: return "01";
            case  2: return "02";
            case  3: return "03";
            case  4: return "04";
            case  5: return "05";
            case  6: return "06";
            case  7: return "07";
            case  8: return "08";
            case  9: return "09";
            case 10: return "10";
            case 11: return "11";
            case 12: return "12";
            case 13: return "13";
            case 14: return "14";
            case 15: return "15";
            case 16: return "16";
            case 17: return "17";
            case 18: return "18";
            case 19: return "19";
            case 20: return "20";
            case 21: return "21";
            case 22: return "22";
            case 23: return "23";
            case 24: return "24";
            case 25: return "25";
            case 26: return "26";
            case 27: return "27";
            case 28: return "28";
            case 29: return "29";
            case 30: return "30";
            case 31: return "31";
            default:
                return String.valueOf(value);
        }
    }

    public static StringBuilder lPad(String s, char padChar, int length)
    {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length - s.length(); i++)
        {
            sb.append(padChar);
        }
        sb.append(s);
        return sb;
    }

    @SuppressWarnings("StringConcatenationInLoop")
    public static String bitString(long l)
    {
        final StringBuilder stringBuilder = lPad(Long.toBinaryString(l), '0', 64);
        String s = "0b";
        for (int i = 0; i < 8; i++)
        {
            for (int j = 0; j < 8; j++)
            {
                char c = stringBuilder.charAt(i * 8 + j);
                s += c;
            }
            s += '_';
        }

        return s.substring(0, s.length() - 1);
    }

    @SuppressWarnings("StringConcatenationInLoop")
    public static String bitString(int l)
    {
        final StringBuilder stringBuilder = lPad(Integer.toBinaryString(l), '0', 32);
        String s = "0b";
        for (int i = 0; i < 4; i++)
        {
            for (int j = 0; j < 8; j++)
            {
                char c = stringBuilder.charAt(i * 8 + j);
                s += c;
            }
            s += '_';
        }

        return s.substring(0, s.length() - 1);
    }
}
