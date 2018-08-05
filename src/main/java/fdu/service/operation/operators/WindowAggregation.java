/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fdu.service.operation.operators;

import fdu.bean.generator.OperatorVisitor;
import fdu.service.operation.UnaryOperation;
import org.json.JSONObject;

/**
 *
 * @author Zhou 
 */
public class WindowAggregation extends UnaryOperation {
	private String name;
    private String windowInterval;
    private String slidingInterval;
    private String function;

    public WindowAggregation(String id, String type, String z) {
        super(id, type, z);
    }
    
    public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getWindowInterval() {
		return windowInterval;
	}

	public void setWindowInterval(String windowInterval) {
		this.windowInterval = windowInterval;
	}

	public String getSlidingInterval() {
		return slidingInterval;
	}

	public void setSlidingInterval(String slidingInterval) {
		this.slidingInterval = slidingInterval;
	}

	public String getFunction() {
		return function;
	}

	public void setFunction(String function) {
		this.function = function;
	}

    @Override
    public void accept(OperatorVisitor visitor) {
        getLeft().accept(visitor);
        visitor.visitWindowAggregation(this);
    }

    public static WindowAggregation newInstance(JSONObject obj){
    	WindowAggregation result = new WindowAggregation(obj.getString("id"), obj.getString("type"), obj.getString("z"));
    	result.setName(obj.getString("name"));
    	result.setWindowInterval(obj.getString("window"));
        result.setSlidingInterval(obj.getString("slide"));
        result.setFunction(obj.getString("function"));
        return result;
    }

	@Override
    public String toString() {
        return "([WindowAggregation" + name + "(" + windowInterval + ", " + slidingInterval 
        		+ ") function: " + function + "]" + getLeft() + ")";
    }
}
