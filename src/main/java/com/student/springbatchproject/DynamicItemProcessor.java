package com.student.springbatchproject;

import org.springframework.batch.item.ItemProcessor;
import java.util.Arrays;

public class DynamicItemProcessor implements ItemProcessor<String[], String[]> {

    @Override
    public String[] process(String[] item) {
        if (item == null) return null;
        for (int i = 0; i < item.length; i++) {
            item[i] = (item[i] == null) ? "" : item[i].trim();
        }
        System.out.println("Reading Row: " + Arrays.toString(item));
        return item;
    }
}
