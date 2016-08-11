package com.lmax.utan;

import com.lmax.utan.web.WebServer;

public class Main
{
    public static void main(String[] args) throws InterruptedException
    {
        try (WebServer webServer = new WebServer(8080))
        {
            webServer.sync();
            System.out.println("Shutting down");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
