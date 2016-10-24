package com.lmax.utan.parser;

@SuppressWarnings("serial")
public final class ParseException extends Exception
{
    private final int location;

    public ParseException(final int location, final String message)
    {
        super(message + " At character: " + location);
        this.location = location;
    }
    
    public int getLocation()
    {
        return location;
    }
    
    @Override
    public Throwable fillInStackTrace()
    {
        return this;
    }
}
