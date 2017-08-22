package com.lmax.utan.parser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static com.lmax.utan.collection.Parse.parseLong;

public class LineParser
{
    private static final int EOF = -1;

    private final LineParserCallback callback;
    private final StringBuilder builder = new StringBuilder();
    private State state = State.PRE_KEY;
    private int position;

    private enum State
    {
        PRE_KEY, IN_KEY, PRE_VALUE, IN_VALUE, PRE_TIMESTAMP, IN_TIMESTAMP, PRE_TAG, IN_TAG_NAME, IN_TAG_VALUE
    }

    public LineParser(final LineParserCallback callback)
    {
        this.callback = callback;
    }

    public void parse(final InputStream in) throws IOException
    {
        initialiseParser();

        int c;

        try
        {
            do
            {
                c = in.read();
                push(c);
            }
            while (c != EOF);
        }
        catch (final ParseException e)
        {
            callback.onFailure(e.getMessage(), e.getLocation());
        }
    }

    public void parse(final ByteBuffer b)
    {
        initialiseParser();

        try
        {
            while (b.hasRemaining())
            {
                push(b.get());
            }

            push(EOF);
        }
        catch (final ParseException e)
        {
            callback.onFailure(e.getMessage(), e.getLocation());
        }
    }

    private void initialiseParser()
    {
        state = State.PRE_KEY;
        position = 0;
        builder.setLength(0);
    }

    @SuppressWarnings({"checkstyle:methodlength", "StatementWithEmptyBody"})
    private void push(final int c) throws ParseException
    {
        switch (state)
        {
            case PRE_KEY:
                if (isKeyChar(c))
                {
                    builder.append((char)c);
                    state = State.IN_KEY;
                }
                else if (!isEndLine(c))
                {
                    throw failure(c);
                }
                break;

            case IN_KEY:
                if (isKeyChar(c))
                {
                    builder.append((char)c);
                }
                else if (isWhiteSpace(c))
                {
                    callback.onKey(builder);
                    builder.setLength(0);
                    state = State.PRE_VALUE;
                }
                else
                {
                    throw failure(c);
                }
                break;

            case PRE_VALUE:
                if (isKeyChar(c))
                {
                    builder.append((char)c);
                    state = State.IN_VALUE;
                }
                else if (isEndLine(c))
                {
                    throw failure(c);
                }
                break;

            case IN_VALUE:
                if (isValueChar(c))
                {
                    builder.append((char)c);
                }
                else if (isWhiteSpace(c))
                {
                    callback.onValue(builder);
                    builder.setLength(0);
                    state = State.PRE_TIMESTAMP;
                }
                else if (isEndLine(c))
                {
                    throw failure(c);
                }
                break;

            case PRE_TIMESTAMP:
                if (isNumber(c))
                {
                    builder.append((char)c);
                    state = State.IN_TIMESTAMP;
                }
                else if (isWhiteSpace(c))
                {
                    // No-op
                }
                else
                {
                    throw failure(c);
                }
                break;

            case IN_TIMESTAMP:
                if (isNumber(c))
                {
                    builder.append((char)c);
                }
                else if (isEndLine(c))
                {
                    final long timestamp = parseLong(builder, 10);
                    callback.onTimestamp(timestamp);
                    state = State.PRE_KEY;
                    callback.onComplete();
                    builder.setLength(0);
                }
                else if (isWhiteSpace(c))
                {
                    final long timestamp = parseLong(builder, 10);
                    callback.onTimestamp(timestamp);
                    state = State.PRE_TAG;
                    builder.setLength(0);
                }
                else
                {
                    throw failure(c);
                }
                break;

            case PRE_TAG:
                if (isValueChar(c))
                {
                    builder.append((char)c);
                    state = State.IN_TAG_NAME;
                }
                break;

            case IN_TAG_NAME:
                if (isValueChar(c))
                {
                    builder.append((char)c);
                }
                else if (isDelimiter(c))
                {
                    callback.onTagName(builder);
                    builder.setLength(0);
                    state = State.IN_TAG_VALUE;
                }
                else if (isWhiteSpace(c) || isEndLine(c))
                {
                    throw failure(c);
                }
                break;

            case IN_TAG_VALUE:
                if (isValueChar(c))
                {
                    builder.append((char)c);
                }
                else if (isEndLine(c))
                {
                    if (builder.length() == 0)
                    {
                        throw failure(c);
                    }
                    callback.onTagValue(builder);
                    builder.setLength(0);
                    callback.onComplete();
                    state = State.PRE_KEY;
                }
                else if (isWhiteSpace(c))
                {
                    if (builder.length() == 0)
                    {
                        throw failure(c);
                    }
                    callback.onTagValue(builder);
                    builder.setLength(0);
                    state = State.PRE_TAG;
                }
                break;

            default:
                break; //throw new ParseException(-1, "Unexpected character '" + c + "'");

        }
        position++;
    }

    private ParseException failure(final int c)
    {
        return new ParseException(position, "Invalid character '" + (char)c + "', state: " + state);
    }

    private boolean isDelimiter(final int c)
    {
        return c == '=';
    }

    private static boolean isEndLine(final int c)
    {
        return c == '\r' || c == '\n' || c == EOF;
    }

    private static boolean isWhiteSpace(final int c)
    {
        return c == ' ' || c == '\t';
    }

    private static boolean isKeyChar(final int c)
    {
        return c == '.' || c == '_' || c == '-' || isNumber(c) || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || c == '(' || c == ')';
    }

    private static boolean isNumber(final int c)
    {
        return ('0' <= c && c <= '9');
    }

    private static boolean isValueChar(final int c)
    {
        return isKeyChar(c);
    }
}
