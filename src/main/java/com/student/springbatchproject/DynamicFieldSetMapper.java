package com.student.springbatchproject;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

public class DynamicFieldSetMapper implements FieldSetMapper<String[]> {

    @Override
    public String[] mapFieldSet(FieldSet fieldSet) throws BindException {
        int count = 0;
        while (true) {
            try {
                fieldSet.readString(count);
                count++;
            } catch (Exception e) {
                break;
            }
        }
        String[] row = new String[count];
        for (int i = 0; i < count; i++) {
            try {
                row[i] = fieldSet.readString(i);
            } catch (Exception ex) {
                row[i] = "";
            }
        }
        return row;
    }
}
