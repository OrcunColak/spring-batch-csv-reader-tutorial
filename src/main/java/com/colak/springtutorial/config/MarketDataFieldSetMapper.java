package com.colak.springtutorial.config;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;

public class MarketDataFieldSetMapper implements FieldSetMapper<MarketData> {

    @Override
    public MarketData mapFieldSet(FieldSet fieldSet) {
        MarketData marketData = new MarketData();
        marketData.setId(fieldSet.readInt("TID"));
        marketData.setTicker(fieldSet.readString("TickerName"));
        marketData.setDescription(fieldSet.readString("TickerDescription"));
        return marketData;
    }
}
