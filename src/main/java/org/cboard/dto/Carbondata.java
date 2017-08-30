package org.cboard.dto;

import java.util.List;

/**
 * Created by frank on 17-8-29.
 */
public class Carbondata {
    private List<String>  columns;

    private String[][] data;

    public Carbondata() {

    }

    public Carbondata(List<String> columns, String[][] data) {
        this.columns = columns;
        this.data = data;
    }

    public List<String> getColumns() {
        return columns;
    }

    public String[][] getData() {
        return data;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public void setData(String[][] data) {
        this.data = data;
    }
}
