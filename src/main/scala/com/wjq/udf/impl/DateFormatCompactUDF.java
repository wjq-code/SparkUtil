package com.wjq.udf.impl;

import com.wjq.udf.IUDF;
import com.wjq.udf.annons.UDF;

@UDF("format_date_compact")
public class DateFormatCompactUDF implements IUDF<String ,String> {

    public String execute(String value) {
        return value.replaceAll("/|-|:| " ,"");
    }
}
