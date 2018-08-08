package fdu.controller;

import fdu.bean.executor.ShellExecutor;
import fdu.bean.generator.ScalaDriverGenerator;
import fdu.service.DataProduceService;
import fdu.service.OperationParserService;
import fdu.service.operation.Operation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;

/**
 * Created by slade on 2016/11/28.
 */
@RestController
public class GeneratorController {

	@Autowired
	private OperationParserService operationParserService;
	@Autowired
	private ShellExecutor shellExecutor;

	private final DataProduceService dataProduceService;

    @Autowired
    public GeneratorController(DataProduceService dataProduceService) {
        this.dataProduceService = dataProduceService;
    }

    @RequestMapping(value = "/runjob", method = RequestMethod.POST)
	public String generateDriver(String config, String masterip, String masterport) throws IOException {
        // start producer
        dataProduceService.startProducer();
		List<Operation> ops = operationParserService.parse(config);
		ScalaDriverGenerator.masterIP = masterip;
        ops.forEach(op -> {
            ScalaDriverGenerator scalaDriverGenerator = new ScalaDriverGenerator();
            op.accept(scalaDriverGenerator);
            String program = scalaDriverGenerator.generate(op.getId());
            try {
                shellExecutor.executeCommand(new String[]{program}, masterip, masterport);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return "ok";
	}

	@RequestMapping(value = "/stopjob", method = RequestMethod.POST)
    public String stopSparkJob() throws IOException {
        String[] cmd = {
                "/bin/sh",
                "-c",
                "jps | grep SparkSubmit | cut -d ' ' -f1 | xargs kill -9"
        };
        shellExecutor.executeCommand(cmd, null, null);
		// stop producer
        dataProduceService.stopProducer();
        return "ok";
    }
}
