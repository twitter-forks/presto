package com.facebook.presto.proxy;

// TODO: make this a interface
public class PrestoSanitizer
{
    boolean checkValidity(String catalog, String schema, String statement) {
        return true;
    }
}
