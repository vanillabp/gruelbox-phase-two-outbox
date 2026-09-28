package io.vanillabp.outbox.gruelbox.it;

import org.springframework.stereotype.Service;

import io.vanillabp.spi.process.ProcessService;
import io.vanillabp.spi.service.WorkflowService;

/**
 * The workflow whose start writes the outbox entries these tests read. It names no BPMN
 * process, so the convention applies and the process id is the name of this class.
 */
@Service
@WorkflowService(workflowAggregateClass = Aggregate.class)
public class SampleWorkflowService {

  private final ProcessService<Aggregate> processService;

  /**
   * @param processService What starts a workflow of this aggregate
   */
  public SampleWorkflowService(
      final ProcessService<Aggregate> processService) {

    this.processService = processService;

  }

  /**
   * @return What starts a workflow of this aggregate
   */
  public ProcessService<Aggregate> getProcessService() {

    return processService;

  }

}
