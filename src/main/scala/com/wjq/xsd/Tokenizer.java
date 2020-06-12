package com.wjq.xsd;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;
import edu.usc.ir.visualization.NLTKandCoreNLP;
import org.apache.tika.parser.ner.nltk.NLTKNERecogniser;
import uk.ac.rdg.resc.edal.time.AllLeapChronology;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 中文分词工具类
 */
public class Tokenizer {

    /**
     * 分词
     */
    public static List<Word> segment(String sentence) {

        //1、 采用HanLP中文自然语言处理中标准分词进行分词
        List<Term> termList = HanLP.segment(sentence);
//        NLTKandCoreNLP nltk = new NLTKandCoreNLP();
        // 采用 NLTK 英文自然语音处理


        //上面控制台打印信息就是这里输出的
        System.out.println(termList.toString());

        //2、重新封装到Word对象中（term.word代表分词后的词语，term.nature代表改词的词性）
        return termList.stream().map(term -> new Word(term.word, term.nature.toString())).collect(Collectors.toList());
    }
}