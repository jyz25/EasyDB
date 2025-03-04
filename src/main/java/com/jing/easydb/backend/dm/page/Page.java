package com.jing.easydb.backend.dm.page;

public interface Page {

    void setDirty(boolean dirty);

    byte[] getData();

    int getPageNumber();

}
