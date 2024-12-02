package com.liangheee.gmall.realtime.dws.util;

import lombok.extern.slf4j.Slf4j;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * IK分词工具类
 * @author liangheee
 * * @date 2024/12/2
 */
@Slf4j
public class IkAnalyzeUtil {
    public static Set<String> splitWord(String word) {
        StringReader stringReader = new StringReader(word);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, true);
        Set<String> keywords = new HashSet<>();
        try {
            Lexeme lexeme;
            while((lexeme = ikSegmenter.next()) != null){
                String keyword = lexeme.getLexemeText();
                keywords.add(keyword);
            }
        }catch (IOException e){
            log.error("IK分词器分词异常：{}",e.getMessage());
        }

        return keywords;
    }
}
