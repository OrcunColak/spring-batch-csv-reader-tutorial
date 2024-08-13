package com.colak.springtutorial.config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class MarketData {

    private int id;

    private String ticker;

    private String description;
}
