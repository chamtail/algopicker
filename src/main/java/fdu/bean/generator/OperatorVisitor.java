package fdu.bean.generator;

import fdu.service.operation.operators.*;

/**
 * Created by slade on 2016/11/24.
 */
public interface OperatorVisitor {
    void visitDataSource(DataSource source);

    void visitFilter(Filter filter);

    void visitWordCount(WordCount wordCount);
    
    void visitWindowAggregation(WindowAggregation windowAggregation);
    
    void visitXC(XC xc);
    
    void visitOutput(Output output);

	String generate(String id);
}
