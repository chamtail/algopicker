package fdu.service.operation.operators;

import org.json.JSONObject;

import fdu.bean.generator.OperatorVisitor;
import fdu.service.operation.UnaryOperation;

public class XC extends UnaryOperation{
	
	public XC(String id, String type, String z) {
		super(id, type, z);
	}
	
	@Override
	public void accept(OperatorVisitor visitor) {
		// TODO Auto-generated method stub
        getLeft().accept(visitor);
        visitor.visitXC(this);
	}

	// TODO: 如何保证每个operation子类都有这个方法
	public static XC newInstance(JSONObject obj) {
		return new XC(obj.getString("id"), obj.getString("type"), obj.getString("z"));
	}
}
