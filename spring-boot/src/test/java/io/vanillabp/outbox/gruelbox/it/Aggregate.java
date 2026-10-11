package io.vanillabp.outbox.gruelbox.it;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * A JPA workflow aggregate with a generated ID which is not a string. The outbox has to
 * serialize the ID as a string, and the phase-two bean has to convert it back.
 */
@Entity
@Table(name = "OUTBOX_TEST_AGGREGATE")
@Getter
@Setter
public class Aggregate {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  private String content;

  /**
   * What a handler of the extension writes while it reports. It is kept apart from the content, so
   * a test can tell the write of the application from the write of a handler.
   */
  private String reported;

}
