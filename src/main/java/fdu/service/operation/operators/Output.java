package fdu.service.operation.operators;

import org.json.JSONObject;

import fdu.bean.generator.OperatorVisitor;
import fdu.service.operation.Operation;
import fdu.service.operation.UnaryOperation;

public class Output extends UnaryOperation{

private String dest;
	
	public String getDest() {
		return dest;
	}

	public void setDest(String dest) {
		this.dest = dest;
	}

	public Output(String id, String type, String z) {
        super(id, type, z);
    }
	
	@Override
    public void accept(OperatorVisitor visitor) {
        getLeft().accept(visitor);
        visitor.visitOutput(this);
    }

	public static Operation newInstance(JSONObject obj) {
		// TODO Auto-generated method stub
		Output result = new Output(obj.getString("id"), obj.getString("type"), obj.getString("z"));
		result.setDest(obj.getString("dest"));
		return result;
	}

}
