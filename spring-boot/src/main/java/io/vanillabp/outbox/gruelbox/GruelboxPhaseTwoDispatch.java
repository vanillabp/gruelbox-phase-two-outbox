package io.vanillabp.outbox.gruelbox;

import io.vanillabp.integration.spi.PhaseTwoCall;

/**
 * The invocation scheduled through the gruelbox transaction outbox, carrying the
 * fields of a {@link PhaseTwoCall}. A dedicated interface is required because
 * gruelbox's <code>DefaultInvocationSerializer</code> only supports a whitelist of
 * parameter types - the call is flattened into {@link String} parameters and rebuilt
 * at dispatch time. Being an interface, it can be proxied by a plain JDK proxy (no
 * ByteBuddy required).
 * <p>
 * The {@link PhaseTwoCall#args()} map is transported GENERICALLY in its serialized
 * form ({@link PhaseTwoCall#serializeArgs(java.util.Map)}) - the store stays
 * operation-agnostic (stores never interpret arguments; only the core's router
 * does).
 */
public interface GruelboxPhaseTwoDispatch {

  /**
   * Dispatches a phase-two call (called by the outbox after the scheduling
   * transaction was committed) by rebuilding the {@link PhaseTwoCall} and routing it
   * through the core's
   * {@link io.vanillabp.integration.adapter.migration.processservice.PhaseTwoRouter}.
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
