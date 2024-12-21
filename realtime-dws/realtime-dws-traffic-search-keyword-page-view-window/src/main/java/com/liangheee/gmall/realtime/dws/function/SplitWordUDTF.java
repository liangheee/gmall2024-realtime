package com.liangheee.gmall.realtime.dws.function;

import com.liangheee.gmall.realtime.dws.util.IkAnalyzeUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * 自定义分词UDTF函数
 * @author liangheee
 * * @date 2024/12/2
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitWordUDTF extends TableFunction<Row> {
    public void eval(String word) {
        for (String keyword : IkAnalyzeUtil.splitWord(word)) {
            // use collect(...) to emit a row
            collect(Row.of(keyword));
        }
    }
}
