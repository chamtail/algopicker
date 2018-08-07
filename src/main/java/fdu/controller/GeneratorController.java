package fdu.controller;

import fdu.bean.executor.ShellExecutor;
import fdu.bean.generator.ScalaDriverGenerator;
import fdu.service.OperationParserService;
import fdu.service.operation.Operation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

/**
 * Created by slade on 2016/11/28.
 */
@RestController
public class GeneratorController {

	@Autowired
	private OperationParserService operationParserService;
	@Autowired
	private ShellExecutor shellExecutor;

	@RequestMapping(value = "/runjob", method = RequestMethod.POST)
	public String generateDriver(String config, String masterip, String masterport,
			ScalaDriverGenerator scalaDriverGenerator) throws IOException {
		Operation op = operationParserService.parse(config);
		scalaDriverGenerator.masterIP = masterip;
		op.accept(scalaDriverGenerator);
		String program = scalaDriverGenerator.generate(op.getId());
		return shellExecutor.executeCommand(program, masterip, masterport);
	}

	@RequestMapping(value = "/stopjob", method = RequestMethod.POST)
    public String stopSparkJob() throws IOException {
        String[] cmd = {
                "/bin/sh",
                "-c",
                "jps | grep SparkSubmit | cut -d ' ' -f1 | xargs kill -9"
        };
        Runtime.getRuntime().exec(cmd);
        return "ok";
    }
}
