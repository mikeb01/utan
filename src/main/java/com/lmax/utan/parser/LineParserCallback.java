package com.lmax.utan.parser;

public interface LineParserCallback
{

    void onKey(CharSequence key);

    void onValue(CharSequence value);

    void onTimestamp(long timestamp);

    void onTagName(CharSequence name);

    void onTagValue(CharSequence value);

    void onComplete();

    void onFailure(String string, int location);

}
