/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fdu.service;

import fdu.Conf;
import fdu.service.operation.BinaryOperation;
import fdu.service.operation.Operation;
import fdu.service.operation.UnaryOperation;
import fdu.service.operation.operators.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author slade
 */
@Service
public class OperationParserService {

	Map<String, OpType> opTypeMap = new HashMap<>();

	public OperationParserService() throws IOException {
		opTypeMap.put(Conf.DATASOURCE, OpType.DATASOURCE);
		opTypeMap.put(Conf.FILTER, OpType.FILTER);
		opTypeMap.put(Conf.WORDCOUNT, OpType.WORDCOUNT);
		opTypeMap.put(Conf.GROUP_BY, OpType.GROUP_BY);
		opTypeMap.put(Conf.WINDOWAGGREGATION, OpType.WINDOWAGGREGATION);
		opTypeMap.put(Conf.TELECOM, OpType.TELECOM);
		opTypeMap.put(Conf.OUTPUT, OpType.OUTPUT);
	}

	private String confWrapper(String conf) {
		return "{ \"infos\": " + conf + "}";
	}

	public List<Operation> parse(String conf) {
		System.out.println(confWrapper(conf));
		JSONObject confJSON = new JSONObject(confWrapper(conf));
		return parseOperation(confJSON);
	}

	enum OpType {
		DATASOURCE, FILTER, WORDCOUNT, WINDOWAGGREGATION, OUTPUT, TELECOM, GROUP_BY
	}

	private List<Operation> parseOperation(JSONObject json) {
		JSONArray infos = (JSONArray) json.get("infos");
		Map<String, Operation> operations = convert2Operations(infos);
		return constructOpTree(infos, operations);
	}

	private List<Operation> constructOpTree(JSONArray infos, Map<String, Operation> operations) {
		List<Operation> roots = new ArrayList<>();
		for (Object obj : infos) {
			if (!(obj instanceof JSONObject)) {
				throw new AssertionError("converting fails: obj is of type " + obj.getClass().getName() + " instead of "
						+ JSONObject.class.getName());
			}
			if ("tab".equals(((JSONObject) obj).get("type"))) { // skip tab json
																// object
				continue;
			}
			
			if (isRoot((JSONObject) obj)) {
				roots.add(getOperationById(operations, (JSONObject) obj));
			} else {
				((JSONObject) obj).getJSONArray("wires").getJSONArray(0).forEach(o -> {
					Operation parent = operations.get(o);
					if (parent instanceof UnaryOperation) {
						if (((UnaryOperation) parent).getLeft() != null) {
							throw new AssertionError("Child already specified.");
						} else {
							((UnaryOperation) parent).setLeft(getOperationById(operations, (JSONObject) obj));
						}
					} else if (parent instanceof BinaryOperation) {
						if (((BinaryOperation) parent).getLeft() != null && ((BinaryOperation) parent).getRight() != null) {
							throw new AssertionError("Both children are specified");
						}

						if (((BinaryOperation) parent).getLeft() == null) {
							((BinaryOperation) parent).setLeft(getOperationById(operations, (JSONObject) obj));
						} else {
							((BinaryOperation) parent).setRight(getOperationById(operations, (JSONObject) obj));
						}
					}
				});
			}
		}

		return roots;
	}
	
	private Operation getOperationById(Map<String, Operation> operations, JSONObject obj) {
        return operations.get(obj.getString("id"));
    }

    private boolean isRoot(JSONObject obj) {
        return obj.getJSONArray("wires").length() == 0 || obj.getJSONArray("wires").getJSONArray(0).length() == 0;
    }

	private Map<String, Operation> convert2Operations(JSONArray infos) {
		Map<String, Operation> result = new HashMap<>();
		for (Object obj : infos) {
			if (!(obj instanceof JSONObject)) {
				throw new AssertionError("converting fails: obj is of type " + obj.getClass().getName() + " instead of "
						+ JSONObject.class.getName());
			}
			if ("tab".equals(((JSONObject) obj).get("type"))) { // skip tab json
																// object
				continue;
			}
			Operation op = convert((JSONObject) obj);
			result.put(op.getId(), op);
		}
		return result;
	}

	private Operation convert(JSONObject obj) {
		Operation result;
		OpType type = opTypeMap.get(obj.getString("type"));
		switch (type) {
		case DATASOURCE:
			result = DataSource.newInstance(obj);
			break;
		case FILTER:
			result = Filter.newInstance(obj);
			break;
		case WORDCOUNT:
		case GROUP_BY:
			result = WordCount.newInstance(obj);
			break;
		case WINDOWAGGREGATION:
			result = WindowAggregation.newInstance(obj);
			break;
		case TELECOM:
			result = XC.newInstance(obj);
			break;
		case OUTPUT:
			result = Output.newInstance(obj);
			break;
		default:
			throw new AssertionError("type " + obj.get("type") + " not exists");
		}
		return result;
	}
}
