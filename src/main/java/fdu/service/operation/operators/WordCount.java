package fdu.service.operation.operators;

import org.json.JSONObject;

import fdu.bean.generator.OperatorVisitor;
import fdu.service.operation.UnaryOperation;

public class WordCount extends UnaryOperation{

	private String name;
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public WordCount(String id, String type, String z) {
        super(id, type, z);
    }
	
	@Override
    public void accept(OperatorVisitor visitor) {
        getLeft().accept(visitor);
        visitor.visitWordCount(this);
    }

	public static WordCount newInstance(JSONObject obj){
		WordCount result = new WordCount(obj.getString("id"), obj.getString("type"), obj.getString("z"));
        result.setName(obj.getString("name"));
        return result;
    }
	
}
