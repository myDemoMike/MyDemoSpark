package com.my.base.json;

import java.io.Serializable;


public class LogGen implements Serializable {


    private String word;

    private String label;

    public void setWord(String word) {
        this.word = word;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getWord() {
        return word;
    }

    public String getLabel() {
        return label;
    }


}


