package io.vanillabp.outbox.gruelbox;

import io.vanillabp.integration.spi.PhaseTwoCall;

/**
 * The invocation scheduled through the gruelbox transaction outbox. It carries the fields of a
 * {@link PhaseTwoCall}. It needs an interface of its own, because the
 * <code>DefaultInvocationSerializer</code> of gruelbox accepts only a fixed list of parameter
 * types. So the call is split into {@link String} parameters and built again at dispatch time. An
 * interface can be proxied by a plain JDK proxy, so no ByteBuddy is needed.
 * <p>
 * The {@link PhaseTwoCall#args()} map travels as a whole, in its serialized form
 * ({@link PhaseTwoCall#serializeArgs(java.util.Map)}). So the store does not need to know the
 * operations. Stores never read the arguments; only the router of the core does.
 */
public interface GruelboxPhaseTwoDispatch {

  /**
   * Dispatches a phase-two call. The outbox calls it after the scheduling transaction was
   * committed. It builds the {@link PhaseTwoCall} again and hands it to the
   * {@link io.vanillabp.integration.adapter.migration.processservice.PhaseTwoRouter} of the core.
   *
   * @param operation The name of the scheduled {@link
   *        io.vanillabp.integration.spi.PhaseOperation}
   * @param workflowModuleId The ID of the workflow module the workflow belongs to
   * @param bpmnProcessId The BPMN process ID of the workflow
   * @param workflowAggregateId The ID of the workflow aggregate in serialized form
   * @param adapterId The ID of the elected BPMS adapter (may be <code>null</code>)
   * @param serializedArgs The call's args map in serialized form (may be
   *        <code>null</code>, see {@link PhaseTwoCall#serializeArgs(java.util.Map)})
   */
  void dispatch(
      String operation,
      String workflowModuleId,
      String bpmnProcessId,
      String workflowAggregateId,
      String adapterId,
      String serializedArgs);

}
